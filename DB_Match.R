library(dplyr)
library(tidyverse)

setwd("C:/Users/320024909/OneDrive - Philips/Team_Members/Preston/SQL_Management")
control_tower <- read.csv("Control_Tower.csv", stringsAsFactors = FALSE, na.strings="")
masters <- read.csv("NA_Masters.csv", stringsAsFactors = FALSE, na.strings="")
deal_boards <- read.csv("NA_DealBoards.csv", stringsAsFactors = FALSE, na.strings="")
links <- read.csv("Links.csv", stringsAsFactors = FALSE, na.strings="")
pilot <- links %>% filter(Opportunity == "Pilot Program for VA Caregiver/Choose Home Ambulatory Telehealth Solution")
new_links <- control_tower %>% select(Opportunity.Name) %>% unique()
colnames(new_links)[1] <- "Opportunity"
new_links <- left_join(new_links, links, by = "Opportunity")
new_links <- rbind(new_links, pilot)
edited_new_links <- new_links %>% select(-Golden_ID)



# Clean NA.Dealboards. We need to spilit any DB looks like 0&1 or 2&3 to individual. We first take care of the combined DB, and then 
# rbind with the regular DBs 
multi_deal_boards <- deal_boards %>% 
  filter(grepl("&", Deal.Board)) %>% 
  separate(Deal.Board, into = c("DB_One", "DB_Two"), sep = "&") 
for(i in 1:nrow(multi_deal_boards)) {
  if (multi_deal_boards[i, "Outcome"] == "In Preparation") {
    multi_deal_boards[i, "DB_One"] <- paste0("p", multi_deal_boards[i, "DB_One"])
    multi_deal_boards[i, "DB_Two"] <- paste0("p", multi_deal_boards[i, "DB_Two"])
  } else {
    multi_deal_boards[i, "DB_One"] <- paste0("a", multi_deal_boards[i, "DB_One"])
    multi_deal_boards[i, "DB_Two"] <- paste0("a", multi_deal_boards[i, "DB_Two"])
  }
}
multi_deal_boards <- cbind(multi_deal_boards[c(1, 4:6)], stack(multi_deal_boards[2:3])) %>% 
  select(-ind) %>%  select(Deal.Board..Deal.Board.Name, values, Deal.Board.Date, Outcome, Golden.Opportunity.ID)
colnames(multi_deal_boards)[2] <- "Deal.Board"

sinlge_deal_boards <- deal_boards %>% filter(!grepl("&", Deal.Board))
deal_boards <- rbind(multi_deal_boards, sinlge_deal_boards)


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
sfdc.all$stage[sfdc.all$stage == "Not Pursuing"] <- "11 - Not Pursuing" #13 - Followed Standard Process; 14 - Pre-Qual Eliminated; 20 Duplicated
sfdc.all$stage[sfdc.all$stage == "Lost"] <- "12 - Lost"
sfdc.all$stage[sfdc.all$stage == "Cancelled by Customer"] <- "15 - Cancelled by Customer"
sfdc.all$db_date <- as.Date(sfdc.all$db_date, format = "%m/%d/%Y") 
sfdc.all$closeE <- as.Date(sfdc.all$closeE, format = "%m/%d/%Y") 
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
  filter(stage == "01 - Pre-Qual" |
         stage == "02 - Qualify" |
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
cTower.all$db_date <- as.Date(cTower.all$db_date, format = "%m/%d/%Y")
cTower.all$closeP <- as.Date(cTower.all$closeP, format = "%m/%d/%Y")
cTower.all$closeA <- as.Date(cTower.all$closeA, format = "%m/%d/%Y")
cTower.all <- cTower.all %>% 
  mutate(sourceCTower = "cTower") 
cTower.focus <- cTower.all %>% 
  filter(stage == "01 - Pre-Qual"|
         stage == "02 - Qualify" |
         stage == "03 - Develop" |
         stage == "04 - Propose" |
         stage == "05 - Negotiation" |
         stage == "06 - Won")

cTower.e6 <- cTower.focus %>% filter(stage != "06 - Won")
cTower.o6 <- cTower.focus %>% filter(stage == "06 - Won")
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
  filter(if_match == FALSE) %>% 
  select(-sourceCTower, -sourceSFDC)
prob <- stage.remind  %>% select(golden_id)
colnames(stage.remind) <- c("Opportunity", "Golden_ID", "CM", "Stage_in_Ctower", "Stage_in_SFDC", "If_Match")
np <- stage.remind %>% filter(Stage_in_SFDC == "11 - Not Pursuing") %>% filter(Stage_in_Ctower == "13 - Followed Standard Process" |
                                                                               Stage_in_Ctower == "14 - Pre-Qual Eliminated"|
                                                                               Stage_in_Ctower == "20 - Duplicate")
stage.remind <- stage.remind[!(stage.remind$Opportunity %in% np$Opportunity),]
stage.remind <- left_join(stage.remind, edited_new_links, by = "Opportunity")
add_links1 <- data.frame(stage.remind[is.na(stage.remind$Master.Oppy.Link) | is.na(stage.remind$Control.Tower.Link), "Opportunity"]) %>% 
  mutate(Reason = "Both")
colnames(add_links1)[1] <- "Opportunity"

# Step 2: check close date in cTower. 
cTower.close.check1 <- cTower.e6 %>% 
  filter(!is.na(closeA)) %>% 
  select(oppy, golden_id, owner, stage, closeA)
cTower.close.check2 <- cTower.o6 %>% 
  filter(is.na(closeA)) %>% 
  select(oppy, golden_id, owner, stage, closeA) %>% 
  unique()
cTower.close.check <- rbind(cTower.close.check1, cTower.close.check2) %>% select(oppy, owner, stage) %>% mutate(miss = "Yes")
colnames(cTower.close.check) <- c("Opportunity", "CM", "Stage_in_Ctower", "Missing_Actual_Closed_Date")
cTower.close.check <- left_join(cTower.close.check, edited_new_links, by = "Opportunity")
add_links2 <- data.frame(cTower.close.check[is.na(cTower.close.check$Control.Tower.Link), "Opportunity"]) %>% 
  mutate(Reason = "CT Link")
colnames(add_links2)[1] <- "Opportunity"


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
colnames(sfdc.close.check) <- c("Opportunity", "Golden_ID", "CM", "Stage", "Expected_Order_Date_in_SFDC", "Actual_Close_Date_in_Ctower", "If_Match")
sfdc.close.check <- left_join(sfdc.close.check, edited_new_links, by = "Opportunity")
add_links3 <- data.frame(sfdc.close.check[is.na(sfdc.close.check$Master.Oppy.Link) | is.na(sfdc.close.check$Control.Tower.Link), "Opportunity"]) %>% 
  mutate(Reason = "Both")
colnames(add_links3)[1] <- "Opportunity"

# Step 4: adding golden_id in control tower list
# once a deal is won, I don't want to remind CM to add id. it doesn't matter 
id.remind <- cTower.focus %>% filter(is.na(golden_id)) %>% 
  select(oppy, golden_id, owner, stage, closeA) %>% unique()
id.remind[is.na(id.remind$closeA), "closeA"] <- Sys.Date()
id.remind <- id.remind %>% filter(closeA > "2017-01-01") %>% select(oppy, golden_id, stage, owner)
colnames(id.remind) <- c("Opportunity", "Golden_ID", "Stage", "CM")
id.remind <- left_join(id.remind, edited_new_links, by = "Opportunity")
add_links4 <- data.frame(id.remind[is.na(id.remind$Control.Tower.Link), "Opportunity"]) %>% 
  mutate(Reason = "CT Link")
colnames(add_links4)[1] <- "Opportunity"

# #NO NEED ANY MORE BECAUSE IT'S SAME AS ID_REMIND 
#Step 5: who only put an oppy in control tower but not in sfdc.
# Again I don't care about other mistakes. I delete the ones whose stage doesn't match
# i.e. Lahey Health LSP is qualified in CT , but PreQual in SFDC. since we only left join cTower.e6 & sfdc.focus,
# we ignore the ones/ don't include the ones that are prequal. therefore it shows NA in sourceSFDC.
# it is not because we forgot creating Lahey in SFDC, it is just because we don't include that deal in sfdc.focus since it's preQual
# oppy.remind <- left_join(cTower.e6, sfdc.focus, by = "golden_id") %>%
#   select(oppy.x, golden_id, owner.x, stage.x, closeA, sourceCTower, sourceSFDC) %>%
#   filter(is.na(sourceSFDC)) %>%
#   unique()
# oppy.remind[is.na(oppy.remind$golden_id), "golden_id"] <- "missing"
# for(i in 1:nrow(prob)) {
#   oppy.remind <- oppy.remind %>% filter(golden_id != prob[i,1])
# }
# oppy.remind <- oppy.remind %>% select(oppy.x, owner.x, sourceCTower, sourceSFDC)
# oppy.remind$sourceCTower <- "Yes"
# oppy.remind$sourceSFDC <- "No"
# colnames(oppy.remind) <- c("opportunity", "CM", "Existed_in_CTower", "Existed_in_SFDC")

# Step 6: dbs that is on control tower but not sfdc. use only control tower's 02-05, 2018-01-01 later deal boards. 
# I don't care about other mistake, just focus on this one. I don't care about 
# for the reason we do prob, please see explanation above in step5
db.remind <- left_join(cTower.db, sfdc.db, by = c("golden_id", "db_date")) %>% 
  select(oppy.x, golden_id, owner.x, db_date, db_num.x, status.x, stage.x, sourceCTower, sourceSFDC)
db.remind <- db.remind[!with(db.remind,is.na(db_date) & is.na(sourceSFDC)),]
db.remind <- db.remind %>% unique() %>% filter(is.na(sourceSFDC))
for(i in 1:nrow(prob)) {
  db.remind <- db.remind %>% filter(golden_id != prob[i,1])
}
db.remind$db_date <- format(db.remind$db_date, "%m-%d-%Y")
db.remind$sourceCTower <- "Yes"
db.remind$sourceSFDC <- "No"
colnames(db.remind) <- c("Opportunity", "Golden_ID", "CM", "DB_Date", "DB_Name", "DB_Status", "Stage", "Existed_in_Ctower", "Existed_in_SFDC")
db.remind <- left_join(db.remind, links, by = "Opportunity")
add_links6 <- data.frame(db.remind[is.na(db.remind$Master.Oppy.Link) | is.na(db.remind$Control.Tower.Link), "Opportunity"]) %>%
  mutate(Reason = "Both")
colnames(add_links6)[1] <- "Opportunity"

# Step7: if a deals is labeled with PreQual, but shows it has already passed DB01, then we should notify CM to correct 
db_stage_match <- control_tower %>% filter(Stage !=  "11 - Not Pursuing") %>% filter(Stage !=  "12 - Lost") %>% filter(Stage !=  "13 - Followed Standard Process") %>% 
  filter(Stage !=  "14 - Pre-Qual Eliminated") %>% filter(Stage !=  "15 - Cancelled by Customer") %>% filter(Stage != "20 - Duplicate") %>% filter(!is.na(Stage)) %>% 
  filter(Stage != "06 - Won") %>% mutate(DB_Stage = NA)
db_stage_match[!is.na(db_stage_match$Actual.DB.3), "DB_Stage"] <- "05 - Negotiation"
db_stage_match[!is.na(db_stage_match$Actual.DB.2) & is.na(db_stage_match$Actual.DB.3), "DB_Stage"] <- "04 - Propose"
db_stage_match[!is.na(db_stage_match$Actual.DB.1) & is.na(db_stage_match$Actual.DB.3) & is.na(db_stage_match$Actual.DB.2), "DB_Stage"] <- "03 - Develop"
db_stage_match[!is.na(db_stage_match$Actual.DB.0) & is.na(db_stage_match$Actual.DB.1) & is.na(db_stage_match$Actual.DB.3) & is.na(db_stage_match$Actual.DB.2), "DB_Stage"] <- "02 - Qualify"
db_stage_match[is.na(db_stage_match$Actual.DB.0) & is.na(db_stage_match$Actual.DB.1) & is.na(db_stage_match$Actual.DB.3) & is.na(db_stage_match$Actual.DB.2), "DB_Stage"] <- "01 - Pre-Qual"
db_stage_match <- db_stage_match %>% filter(Stage != DB_Stage) %>% select(Opportunity.Name, Capture.Manager, Stage, DB_Stage)
colnames(db_stage_match) <- c("Opportunity", "CM", "Stage_Typed_in_CTower", "Stage_Based_on_DB_Passed_Record")
db_stage_match <- left_join(db_stage_match, edited_new_links, by = "Opportunity")
add_links7 <- data.frame(db_stage_match[is.na(db_stage_match$Control.Tower.Link), "Opportunity"]) %>% mutate(Reason = "CT Link")
colnames(add_links7)[1] <- "Opportunity"

# Step8: the active deals last year, we need to correct it's planned close date to current year 
prev_active <- cTower.e6 %>% select(owner, oppy, stage, closeP) %>% unique() %>% 
  filter(!is.na(closeP), closeP < as.Date("2019-01-01", format = "%Y-%m-%d"))
colnames(prev_active) <- c("CM", "Opportunity", "Stage_in_Ctower", "Planned_Close_Date")
prev_active <- left_join(prev_active, edited_new_links, by = "Opportunity")
add_links8 <- data.frame(prev_active[is.na(prev_active$Control.Tower.Link), "Opportunity"]) %>% 
  mutate(Reason = "CT Link")
colnames(add_links8)[1] <- "Opportunity"


# Step9: if there's anything with an ISC True, but LSP false. use master data
isc_lsp <- masters %>% select(Opportunity.Name, Golden.Opportunity.ID, Stage, Opportunity.Owner, Integrated.Solution.Center..ISC., LSP.Solution)
colnames(isc_lsp) <- c("Opportunity", "Golden_ID", "Stage","CM","If_ISC","If_LSP")
isc_lsp <- isc_lsp %>% filter(!is.na(If_ISC))
isc_lsp <- isc_lsp %>% filter(!is.na(If_LSP))
isc_lsp[isc_lsp$If_ISC == 0, "If_ISC"] <- "No"
isc_lsp[isc_lsp$If_ISC == 1, "If_ISC"] <- "Yes"
isc_lsp[isc_lsp$If_LSP == 0, "If_LSP"] <- "No"
isc_lsp[isc_lsp$If_LSP == 1, "If_LSP"] <- "Yes"
isc_lsp_remind <- isc_lsp %>% filter(If_ISC == "Yes",If_LSP == "No") %>% 
  filter(Stage != "Closing Signed, Won (Master)"&
           Stage != "Lost"&
           Stage != "Cancelled by Customer"&
           Stage != "Not Pursuing")
isc_lsp_remind <- left_join(isc_lsp_remind, new_links, by = "Golden_ID")
add_links9 <- data.frame(isc_lsp_remind[is.na(prev_active$Master.Oppy.Link), "Opportunity"]) %>% 
  mutate(Reason = "SFDC")
colnames(add_links9)[1] <- "Opportunity"

# edit the links! you just need to change the old name of the opportunity on old links.csv to the new name. that's it!
# Idealy, the opportunity name in CT and SFDC should be same. If it keeps reminding you about an oppy but you're sure that is on the Link.csv. 
# Then go check if CT, SFDC, and LINK they all have the same opportunity name 
# Talk with Preston about those opportunity name 
rbind(add_links1, add_links2, add_links3, add_links4, add_links6, add_links7, add_links8, add_links9) %>% unique()

setwd("C:/Users/320024909/OneDrive - Philips/Team_Members/Preston/SQL_Management")
write.csv(stage.remind, "Stage_Remind.csv") # step 1
write.csv(cTower.close.check, "CTower_Close_Check.csv") # step 2
write.csv(sfdc.close.check, "Sfdc_Close_Check.csv") # step 3
write.csv(id.remind, "ID_Remind.csv") # step 4
#write.csv(oppy.remind, "Oppy_Remind.csv") # step NO NEED. SAME AS STEP4 ID REMIND
write.csv(db.remind, "DB_Remind.csv") # step 6
write.csv(db_stage_match, "DB_Stage_Remind.csv") # step 7
write.csv(prev_active, "Prev_Active_Remind.csv") # step 8
write.csv(isc_lsp_remind,"ISC_LSP_Remind.csv") # step 9


      