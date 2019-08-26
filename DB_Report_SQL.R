library(dplyr)
library(reshape)
library(RODBC)
library(reshape2)

conn <- odbcDriverConnect("Driver={SQL Server};Server=QQ0002UA3411755;Database=isc;Trusted_Connection=Yes")

control_tower <- sqlFetch(conn, "tblControlTower", stringsAsFactors = FALSE)
names(control_tower) <- gsub(" ", ".", names(control_tower), fixed = TRUE)
control_tower$Planned.DB.0 <- as.character(control_tower$Planned.DB.0)
control_tower$Planned.DB.1 <- as.character(control_tower$Planned.DB.1)
control_tower$Planned.DB.2 <- as.character(control_tower$Planned.DB.2)
control_tower$Planned.DB.3 <- as.character(control_tower$Planned.DB.3)
control_tower$Planned.DB.4 <- as.character(control_tower$Planned.DB.4)
control_tower$Actual.DB.0 <- as.character(control_tower$Actual.DB.0)
control_tower$Actual.DB.1 <- as.character(control_tower$Actual.DB.1)
control_tower$Actual.DB.2 <- as.character(control_tower$Actual.DB.2)
control_tower$Actual.DB.3 <- as.character(control_tower$Actual.DB.3)
control_tower$Actual.DB.4 <- as.character(control_tower$Actual.DB.4)
control_tower$Planned.Close.Date <- as.character(control_tower$Planned.Close.Date)
control_tower$Actual.Close.Date <- as.character(control_tower$Actual.Close.Date)


masters <- sqlFetch(conn, "tblMasters",stringsAsFactors = FALSE)
names(masters) <- gsub(" ", '.', names(masters), fixed = T)
masters$Expected.Order.Date <- as.character(masters$Expected.Order.Date)


deal_boards <- sqlFetch(conn, "tblDealBoards",stringsAsFactors = FALSE)
names(deal_boards) <- gsub(" ", '.', names(deal_boards), fixed = T)
deal_boards$Deal.Board.Date <- as.character(deal_boards$Deal.Board.Date)
deal_boards$Deal.Board <- as.character(deal_boards$Deal.Board)
deal_boards$Deal.Board <- as.character(deal_boards$Deal.Board)


# this code helps to identify if there's any mismatch of deal boards records on SP and SFDC. 
# for every db we have in control tower with "actual date", we want to include it on SFDC with the same date, approved status,
# for the db with "planned date", we want to include it on SFDC with the same date, in preparation status
# we must make sure that the oppy in Control Tower must also exist in SFDC

# all oppys and deal boards in SFDC 
sfdc.all <- left_join(masters, deal_boards, by = "Golden.Opportunity.ID") %>% 
  select(oppy = Opportunity.Name,
         golden_id = Golden.Opportunity.ID,
         owner = Opportunity.Owner,
         stage = Stage,
         closeE = Expected.Order.Date,
         db_date = Deal.Board.Date,
         db_num = Deal.Board,
         status = Outcome)
sfdc.all$stage[sfdc.all$stage == "Pre-Qualification (Master)"] <- "01 - Pre-Qual"
sfdc.all$stage[sfdc.all$stage == "Qualify (Master)"] <- "02 - Qualify"
sfdc.all$stage[sfdc.all$stage == "Develop (Master)"] <- "03 - Develop"   
sfdc.all$stage[sfdc.all$stage == "Propose (Master)"] <- "04 - Propose" 
sfdc.all$stage[sfdc.all$stage == "Negotiation (Master)"] <- "05 - Negotiation" 
sfdc.all$stage[sfdc.all$stage == "Closing Signed, Won (Master)"] <- "06 - Won" 
sfdc.all$stage[sfdc.all$stage == "Not Pursuing"] <- "11 - Not Pursuing" #13 - Followed Standard Process; 14 - Pre-Qual Eliminated
sfdc.all$stage[sfdc.all$stage == "Lost"] <- "12 - Lost"
sfdc.all$stage[sfdc.all$stage == "Cancelled by Customer"] <- "15 - Cancelled by Customer"
sfdc.all$db_date <- as.Date(sfdc.all$db_date) 
sfdc.all$closeE <- as.Date(sfdc.all$closeE) 
sfdc.all <- sfdc.all %>% mutate(sourceSFDC = "sfdc")

label <- function(x) {
  if (is.na(x[,"status"])) {
    x[,"db_num"] = NA
  } else if (x[,"status"] == "In Preparation") {
    x[,"db_num"] = paste0("p", x[,"db_num"])
  } else {
    x[,"db_num"] = paste0("a", x[,"db_num"])
  } 
  return(x)
}
for (i in 1:nrow(sfdc.all)) {
  sfdc.all[i,] <- label(sfdc.all[i,])
}

sfdc.focus <- sfdc.all %>%
  filter(stage == "02 - Qualify" |
           stage == "03 - Develop" |
           stage == "04 - Propose" |
           stage == "05 - Negotiation" |
           stage == "06 - Won")
sfdc.e6 <- sfdc.focus %>% filter(stage != "06 - Won") 
sfdc.o6 <- sfdc.focus %>% filter(stage == "06 - Won")
sfdc.db <- sfdc.e6 %>% filter(db_date > "2018-01-01")

# all the oppy records in control tower. there are many blanks that leaves for no reason and we have no intereste in reminding CMs 
# to fill out. I intentionally leave them there because I want to keep the ones that has blank but actually have record in sfdc. 
cTower.all <- control_tower %>% select(oppy = Opportunity.Name, 
                                       golden_id = Golden.Oppy.ID,
                                       owner = Capture.Manager,
                                       stage = Stage,
                                       p0 = Planned.DB.0,
                                       p1 = Planned.DB.1,
                                       p2 = Planned.DB.2,
                                       p3 = Planned.DB.3,
                                       p4 = Planned.DB.4,
                                       a0 = Actual.DB.0,
                                       a1 = Actual.DB.1,
                                       a2 = Actual.DB.2,
                                       a3 = Actual.DB.3,
                                       a4 = Actual.DB.4,
                                       closeP = Planned.Close.Date,
                                       closeA = Actual.Close.Date)

# for each row, once there's a date in a0, there should be NA in p0
eliminate <- function(x) {
  if (!is.na(x[,"a0"])) {
    x[,"p0"] = NA
  }
  if (!is.na(x[,"a1"])) {
    x["p1"] = NA
  }
  if (!is.na(x[,"a2"])) {
    x["p2"] = NA
  }
  if (!is.na(x[,"a3"])) {
    x["p3"] = NA
  }
  if (!is.na(x[,"a4"])) {
    x["p4"] = NA
  }
  return(x)
}

for (i in 1:nrow(cTower.all)) {
  cTower.all[i,] <- eliminate(cTower.all[i,])
}

cTower.all <- cbind(cTower.all[c(1:4, 15:16)], stack(cTower.all[5:14])) # very useful! like it!  
cTower.all$ind <- as.character(cTower.all$ind)
cTower.all <- cTower.all %>% mutate(status = NA)
cTower.all$status[cTower.all$ind == "p0" |
                    cTower.all$ind == "p1" |
                    cTower.all$ind == "p2" |
                    cTower.all$ind == "p3" |
                    cTower.all$ind == "p4"] <- "In Preparation"
cTower.all$status[cTower.all$ind == "a0" |
                    cTower.all$ind == "a1" |
                    cTower.all$ind == "a2" |
                    cTower.all$ind == "a3" |
                    cTower.all$ind == "a4"] <- "Approved"
colnames(cTower.all)[7:9] <- c("db_date", "db_num", "status")
cTower.all$db_date <- as.Date(cTower.all$db_date)
cTower.all$closeP <- as.Date(cTower.all$closeP)
cTower.all$closeA <- as.Date(cTower.all$closeA)
cTower.all <- cTower.all %>% 
  mutate(sourceCTower = "cTower") %>% 
  filter(stage == "02 - Qualify" |
           stage == "03 - Develop" |
           stage == "04 - Propose" |
           stage == "05 - Negotiation" |
           stage == "06 - Won")

cTower.e6 <- cTower.all %>% filter(stage != "06 - Won")
cTower.o6 <- cTower.all %>% filter(stage == "06 - Won")
cTower.db <- cTower.e6 %>% filter(db_date > "2018-01-01")

close_combine <- cTower.o6 %>% filter(!is.na(closeA), closeA > "2018-01-01") %>% select(golden_id, closeA)


# Keep in mind that we only interested in reminding the oppy in control tower. 
# Step 1: compare stages. I don't care about the deals that in sfdc, or any closed deals.
# I only want remind the oppy with stage 02-05, to change to corrent ones in sfdc
# i also exclude the one without golden_id
stage.remind <- left_join(cTower.all, sfdc.all, by = "golden_id") %>% 
  select(oppy.x, golden_id, owner.x, stage.x, stage.y, sourceCTower, sourceSFDC) %>% 
  filter(!is.na(golden_id), !is.na(sourceSFDC)) %>% 
  unique()
stage.remind <- stage.remind %>% 
  mutate(if_match = stage.remind$stage.x == stage.remind$stage.y) %>% 
  filter(if_match == FALSE)

# Step 2: check close date in cTower. 
# if there's stage 02-05 deals with an actual close date. 
cTower.close.check1 <- cTower.e6 %>% 
  filter(!is.na(closeA)) %>% 
  select(oppy, golden_id, owner, stage, closeA)
# the won deals that don't have close date 
cTower.close.check2 <- cTower.o6 %>% 
  filter(is.na(closeA)) %>% 
  select(oppy, golden_id, owner, stage, closeA) %>% 
  unique()
# combine togethr
cTower.close.check <- rbind(cTower.close.check1, cTower.close.check2)


# Step 3: find out if CM input the date as expected order date in sfdc as they input in cTower.
# i don't care about other mistake. just use sfdc won 06 stage oppy. if an oppy mislabel, it should be o6, 
# but it is labeled sth else, I don't care. That would be find in previous page. 
sfdc.close.check <- left_join(sfdc.o6, close_combine, by = "golden_id") %>% 
  select(oppy, golden_id, owner, stage, closeE, closeA) %>% 
  filter(closeA > "2018-01-01") %>%
  mutate(if_match = closeA - closeE) %>% 
  unique()
sfdc.close.check$if_match <- as.integer(sfdc.close.check$if_match)
sfdc.close.check[sfdc.close.check$if_match != 0, "if_match"] <- "No" 
sfdc.close.check <- sfdc.close.check %>% filter(if_match == "No")
sfdc.close.check$closeE <- as.character(sfdc.close.check$closeE)
sfdc.close.check$closeA <- as.character(sfdc.close.check$closeA)


# Step 4: adding golden_id in control tower list
# once a deal is won, I don't want to remind CM to add id. it doesn't matter 
id.remind <- cTower.all %>% filter(is.na(golden_id)) %>% 
  select(oppy, golden_id, owner, stage, closeA) %>% unique()
id.remind[is.na(id.remind$closeA), "closeA"] <- Sys.Date()
id.remind <- id.remind %>% filter(closeA > "2017-01-01")
id.remind <- id.remind %>% select(oppy, golden_id, owner)


# Step 5: who only put an oppy in control tower but not in sfdc.
# Again I don't care about other mistakes. I delete the ones whose stage doesn't match 
oppy.remind <- left_join(cTower.e6, sfdc.focus, by = "golden_id") %>% 
  select(oppy.x, golden_id, owner.x, stage.x, closeA, sourceCTower, sourceSFDC) %>% 
  filter(is.na(sourceSFDC)) %>% 
  unique()
oppy.remind[is.na(oppy.remind$golden_id), "golden_id"] <- "missing"
prob <- stage.remind  %>% select(golden_id)
for(i in 1:nrow(prob)) {
  oppy.remind <- oppy.remind %>% filter(golden_id != prob[i,1])
}
oppy.remind <- oppy.remind %>% select(-closeA)

# Step 6: dbs that is on control tower but not sfdc. use only control tower's 02-05, 2018-01-01 later deal boards. 
# I don't care about other mistake, just focus on this one. I don't care about 
db.remind <- left_join(cTower.db, sfdc.db, by = c("golden_id", "db_date")) %>% 
  select(oppy.x, golden_id, owner.x, db_date, db_num.x, status.x, stage.x, sourceCTower, sourceSFDC)
db.remind <- db.remind[!with(db.remind,is.na(db_date) & is.na(sourceSFDC)),]
db.remind <- db.remind %>% unique() %>% filter(is.na(sourceSFDC))
for(i in 1:nrow(prob)) {
  db.remind <- db.remind %>% filter(golden_id != prob[i,1])
}
db.remind$db_date <- format(db.remind$db_date, "%Y-%m-%d")
db.remind$db_date <- as.character(db.remind$db_date)


# sqlSave(conn, stage.remind, tablename = "tblStageRemind", rownames = TRUE)
# sqlSave(conn, cTower.close.check, tablename = "tblCTowerCloseCheck", rownames = TRUE)
# sqlSave(conn, sfdc.close.check, tablename = "tblSfdcCloseCheck", rownames = TRUE)
# sqlSave(conn, id.remind, tablename = "tblIdRemind", rownames = TRUE)
# sqlSave(conn, oppy.remind, tablename = "tblOppyRemind", rownames = TRUE)
# sqlSave(conn, db.remind, tablename = "tblDbRemind", rownames = TRUE)

sqlUpdate(conn, stage.remind, tablename = "tblStageRemind") 
sqlUpdate(conn, cTower.close.check, tablename = "tblCTowerCloseCheck") 
sqlUpdate(conn, sfdc.close.check, tablename = "tblSfdcCloseCheck") 
sqlUpdate(conn, id.remind, tablename = "tblIdRemind") 
sqlUpdate(conn, oppy.remind, tablename = "tblOppyRemind") 
sqlUpdate(conn, db.remind, tablename = "tblDbRemind") 

