install.packages("bigrquery")
install.packages("pak")
install.packages("arsenal")
install.packages("readr")
pak::pak("r-dbi/bigrquery")

## Description =================================================================
# Title:        Script Title
# Author:       Brittany Crawford
# Date:         2024-09-25
# Objective:    Calculate physical activity ROI

## Script Parameters ===========================================================
# Use can modify these
dataset <- "FlatConnect"
table   <- "module2_v1_JP"
tier    <- "prod" # "dev", "stg", or "prod"

project <- switch(tier,
                  dev  = "nih-nci-dceg-connect-dev",
                  stg  = "nih-nci-dceg-connect-stg-5519",
                  prod = "nih-nci-dceg-connect-prod-6d04")

billing <- project # Use the same project for billing, always.

library(dplyr)
library(DBI)
library(bigrquery)
library(glue)
library(arsenal)
library(readr)
library(foreach)
library(stringr)
#library(plyr)
#library(expss) ###to add labels
library(epiDisplay) ##recommended applied here crosstable, tab1
library(gmodels) ##recommended
library(magrittr)
library(gtsummary)
library(rio)

library(ggplot2)
library(gridExtra)
library(scales)
library(gt)
library(tinytex)
library(data.table) ###to write or read and data management 
library(tidyverse) ###for data management
library(reshape)  ###to work on transition from long to wide or wide to long data
library(listr) ###to work on a list of vector, files or..
library(sqldf) ##sql
library(lubridate) ###date time
library(kableExtra)

## Connect to Database =========================================================
bq_auth() # Authenticate with BigQuery

# Establish connection to BigQuery
con <- dbConnect(bigrquery::bigquery(), 
                 project=project, 
                 dataset=dataset, 
                 billing=billing)

# Specify just the data we need with a query
sql <- glue(
  "WITH m2_dup AS
(SELECT
  Connect_ID, 
  D_517976064_D_904954920 AS SrvMRE_WalkHike_v1r0,
  D_517976064_D_619501806 AS SrvMRE_JogRun_v1r0,
  D_517976064_D_203192394 AS SrvMRE_Tennis_v1r0,
  D_517976064_D_261267696 AS SrvMRE_PlayGolf_v1r0,
  D_517976064_D_926584500 AS SrvMRE_SwimLaps_v1r0,
  D_517976064_D_420058896 AS SrvMRE_BikeRide_v1r0,
  D_517976064_D_868685663 AS SrvMRE_Strengthening_v1r0,
  D_517976064_D_760484278 AS SrvMRE_Yoga_v1r0,
  D_517976064_D_345916806 AS SrvMRE_MartialArts_v1r0,
  D_517976064_D_936042582 AS SrvMRE_Dance_v1r0,
  D_517976064_D_182827107 AS SrvMRE_DownhillSki_v1r0,
  D_517976064_D_734860227 AS SrvMRE_CrossCountry_v1r0,
  D_517976064_D_371531887 AS SrvMRE_Surf_v1r0,
  D_517976064_D_423631576 AS SrvMRE_HICT_v1r0,
  D_517976064_D_181769837 AS SrvMRE_OtherExercise_v1r0,
  D_517976064_D_535003378 AS SrvMRE_None_v1r0,
  D_267122668 AS SrvMRE_WalkHikeOften_v1r0,
  D_901660173 AS SrvMRE_WalkHikeTime_v1r0,
  D_953510929 AS SrvMRE_JogRunOften_v1r0,
  D_422260069 AS SrvMRE_JogRunTime_v1r0,
  D_411788467 AS SrvMRE_TennisOften_v1r0,
  D_141251197 AS SrvMRE_TennisTime_v1r0,
  D_184448791 AS SrvMRE_GolfOften_v1r0,
  D_768302347 AS SrvMRE_GolfTime_v1r0,
  D_944699052 AS SrvMRE_SwimLapsOften_v1r0,
  D_658018390 AS SrvMRE_SwimLapsTime_v1r0,
  D_849399881 AS SrvMRE_BikeOften_v1r0,
  D_406846149 AS SrvMRE_BikeTime_v1r0,
  D_689956879 AS SrvMRE_StrengthOften_v1r0,
  D_725713485 AS SrvMRE_StrengthTime_v1r0,
  D_255761998 AS SrvMRE_YogaOften_v1r0,
  D_571361258 AS SrvMRE_YogaTime_v1r0,
  D_167966775 AS SrvMRE_MAOften_v1r0,
  D_904844824 AS SrvMRE_MATime_v1r0,
  D_612068433 AS SrvMRE_DanceOften_v1r0,
  D_650405110 AS SrvMRE_DanceTime_v1r0,
  D_865200503 AS SrvMRE_SkiOften_v1r0,
  D_831643763 AS SrvMRE_SkiTime_v1r0,
  D_981509686 AS SrvMRE_CCSkiOften_v1r0,
  D_921220809 AS SrvMRE_CCSkiTime_v1r0,
  D_214750556 AS SrvMRE_SurfOften_v1r0,
  D_621878019 AS SrvMRE_SurfTime_v1r0,
  D_775324618 AS SrvMRE_HICTOften_v1r0,
  D_305312165 AS SrvMRE_HICTTime_v1r0,
  D_522949496 AS SrvMRE_ExerciseOften_v1r0,
  D_272119228 AS SrvMRE_ExerciseTime_v1r0,
  
  D_894610280_D_152773041 AS SrvMRE_WalkHikeSpring_v1r0,
  D_894610280_D_249341444	AS SrvMRE_WalkHikeSummer_v1r0,
  D_894610280_D_690018400 AS SrvMRE_WalkHikeFall_v1r0,
  D_894610280_D_917302906 AS SrvMRE_WalkHikeWinter_v1r0,
  D_222110888_D_152773041 AS SrvMRE_JogRunSpring_v1r0,
  D_222110888_D_249341444 AS SrvMRE_JogRunSummer_v1r0,
  D_222110888_D_690018400 AS SrvMRE_JogRunFall_v1r0,
  D_222110888_D_917302906 AS SrvMRE_JogRunWinter_v1r0,
  D_564242877_D_152773041 AS SrvMRE_TennisSpring_v1r0,
  D_564242877_D_249341444	AS SrvMRE_TennisSummer_v1r0,
  D_564242877_D_690018400 AS SrvMRE_TennisFall_v1r0,
  D_564242877_D_917302906 AS SrvMRE_TennisWinter_v1r0,
  D_635874413_D_152773041 AS SrvMRE_GolfSpring_v1r0,
  D_635874413_D_249341444 AS SrvMRE_GolfSummer_v1r0,
  D_635874413_D_690018400 AS SrvMRE_GolfFall_v1r0,
  D_635874413_D_917302906 AS SrvMRE_GolfWinter_v1r0,
  D_371748514_D_152773041 AS SrvMRE_SwimLapsSpring_v1r0,
  D_371748514_D_249341444 AS SrvMRE_SwimLapsSummer_v1r0,
  D_371748514_D_690018400 AS SrvMRE_SwimLapsFall_v1r0,
  D_371748514_D_917302906 AS SrvMRE_SwimLapsWinter_v1r0,
  D_858525957_D_152773041 AS SrvMRE_BikeSpring_v1r0,
  D_858525957_D_249341444 AS SrvMRE_BikeSummer_v1r0,
  D_858525957_D_690018400 AS SrvMRE_BikeFall_v1r0,
  D_858525957_D_917302906 AS SrvMRE_BikeWinter_v1r0,
  D_787591805_D_152773041 AS SrvMRE_StrengthSpring_v1r0,
  D_787591805_D_249341444 AS SrvMRE_StrengthSummer_v1r0,
  D_787591805_D_690018400 AS SrvMRE_StrengthFall_v1r0,
  D_787591805_D_917302906 AS SrvMRE_StrengthWinter_v1r0,
  D_900299856_D_152773041 AS SrvMRE_YogaSpring_v1r0, 
  D_900299856_D_249341444 AS SrvMRE_YogaSummer_v1r0,
  D_900299856_D_690018400 AS SrvMRE_YogaFall_v1r0,
  D_900299856_D_917302906 AS SrvMRE_YogaWinter_v1r0,
  D_149884127_D_152773041 AS SrvMRE_MASpring_v1r0,
  D_149884127_D_249341444 AS SrvMRE_MASummer_v1r0,
  D_149884127_D_690018400 AS SrvMRE_MAFall_v1r0,
  D_149884127_D_917302906 AS SrvMRE_MAWinter_v1r0,
  D_845164425_D_152773041 AS SrvMRE_DanceSpring_v1r0,
  D_845164425_D_249341444 AS SrvMRE_DanceSummer_v1r0,
  D_845164425_D_690018400 AS SrvMRE_DanceFall_v1r0,
  D_845164425_D_917302906 AS SrvMRE_DanceWinter_v1r0,
  D_187772368_D_152773041 AS SrvMRE_SkiSpring_v1r0,
  D_187772368_D_249341444 AS SrvMRE_SkiSummer_v1r0,
  D_187772368_D_690018400 AS SrvMRE_SkiFall_v1r0,
  D_187772368_D_917302906 AS SrvMRE_SkiWinter_v1r0,
  D_815229596_D_152773041 AS SrvMRE_CCSkiSpring_v1r0,
  D_815229596_D_249341444 AS SrvMRE_CCSkiSummer_v1r0,
  D_815229596_D_690018400 AS SrvMRE_CCSkiFall_v1r0,
  D_815229596_D_917302906 AS SrvMRE_CCSkiWinter_v1r0,
  D_262305264_D_152773041 AS SrvMRE_SurfSpring_v1r0,
  D_262305264_D_249341444 AS SrvMRE_SurfSummer_v1r0,
  D_262305264_D_690018400 AS SrvMRE_SurfFall_v1r0,
  D_262305264_D_917302906 AS SrvMRE_SurfWinter_v1r0,
  D_409324592_D_152773041 AS SrvMRE_HICTSpring_v1r0,
  D_409324592_D_249341444 AS SrvMRE_HICTSummer_v1r0,
  D_409324592_D_690018400 AS SrvMRE_HICTFall_v1r0,
  D_409324592_D_917302906 AS SrvMRE_HICTWinter_v1r0,
  D_895837106_D_152773041 AS SrvMRE_OtherExerciseSpring_v1r0,
  D_895837106_D_249341444 AS SrvMRE_OtherExerciseSummer_v1r0,
  D_895837106_D_690018400 AS SrvMRE_OtherExerciseFall_v1r0,
  D_895837106_D_917302906 AS SrvMRE_OtherExerciseWinter_v1r0,
  2 as version
  FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.module2_v2_JP`
  UNION ALL
  SELECT 
  Connect_ID,
    D_517976064_D_904954920 AS SrvMRE_WalkHike_v1r0,
  D_517976064_D_619501806 AS SrvMRE_JogRun_v1r0,
  D_517976064_D_203192394 AS SrvMRE_Tennis_v1r0,
  D_517976064_D_261267696 AS SrvMRE_PlayGolf_v1r0,
  D_517976064_D_926584500 AS SrvMRE_SwimLaps_v1r0,
  D_517976064_D_420058896 AS SrvMRE_BikeRide_v1r0,
  D_517976064_D_868685663 AS SrvMRE_Strengthening_v1r0,
  D_517976064_D_760484278 AS SrvMRE_Yoga_v1r0,
  D_517976064_D_345916806 AS SrvMRE_MartialArts_v1r0,
  D_517976064_D_936042582 AS SrvMRE_Dance_v1r0,
  D_517976064_D_182827107 AS SrvMRE_DownhillSki_v1r0,
  D_517976064_D_734860227 AS SrvMRE_CrossCountry_v1r0,
  D_517976064_D_371531887 AS SrvMRE_Surf_v1r0,
  D_517976064_D_423631576 AS SrvMRE_HICT_v1r0,
  D_517976064_D_181769837 AS SrvMRE_OtherExercise_v1r0,
  D_517976064_D_535003378 AS SrvMRE_None_v1r0,
  D_267122668 AS SrvMRE_WalkHikeOften_v1r0,
  D_901660173 AS SrvMRE_WalkHikeTime_v1r0,
  D_953510929 AS SrvMRE_JogRunOften_v1r0,
  D_422260069 AS SrvMRE_JogRunTime_v1r0,
  D_411788467 AS SrvMRE_TennisOften_v1r0,
  D_141251197 AS SrvMRE_TennisTime_v1r0,
  D_184448791 AS SrvMRE_GolfOften_v1r0,
  D_768302347 AS SrvMRE_GolfTime_v1r0,
  D_944699052 AS SrvMRE_SwimLapsOften_v1r0,
  D_658018390 AS SrvMRE_SwimLapsTime_v1r0,
  D_849399881 AS SrvMRE_BikeOften_v1r0,
  D_406846149 AS SrvMRE_BikeTime_v1r0,
  D_689956879 AS SrvMRE_StrengthOften_v1r0,
  D_725713485 AS SrvMRE_StrengthTime_v1r0,
  D_255761998 AS SrvMRE_YogaOften_v1r0,
  D_571361258 AS SrvMRE_YogaTime_v1r0,
  D_167966775 AS SrvMRE_MAOften_v1r0,
  D_904844824 AS SrvMRE_MATime_v1r0,
  D_612068433 AS SrvMRE_DanceOften_v1r0,
  D_650405110 AS SrvMRE_DanceTime_v1r0,
  D_865200503 AS SrvMRE_SkiOften_v1r0,
  D_831643763 AS SrvMRE_SkiTime_v1r0,
  D_981509686 AS SrvMRE_CCSkiOften_v1r0,
  D_921220809 AS SrvMRE_CCSkiTime_v1r0,
  D_214750556 AS SrvMRE_SurfOften_v1r0,
  D_621878019 AS SrvMRE_SurfTime_v1r0,
  D_775324618 AS SrvMRE_HICTOften_v1r0,
  D_305312165 AS SrvMRE_HICTTime_v1r0,
  D_522949496 AS SrvMRE_ExerciseOften_v1r0,
  D_272119228 AS SrvMRE_ExerciseTime_v1r0,
  
  D_894610280_D_152773041 AS SrvMRE_WalkHikeSpring_v1r0,
  D_894610280_D_249341444	AS SrvMRE_WalkHikeSummer_v1r0,
  D_894610280_D_690018400 AS SrvMRE_WalkHikeFall_v1r0,
  D_894610280_D_917302906 AS SrvMRE_WalkHikeWinter_v1r0,
  D_222110888_D_152773041 AS SrvMRE_JogRunSpring_v1r0,
  D_222110888_D_249341444 AS SrvMRE_JogRunSummer_v1r0,
  D_222110888_D_690018400 AS SrvMRE_JogRunFall_v1r0,
  D_222110888_D_917302906 AS SrvMRE_JogRunWinter_v1r0,
  D_564242877_D_152773041 AS SrvMRE_TennisSpring_v1r0,
  D_564242877_D_249341444	AS SrvMRE_TennisSummer_v1r0,
  D_564242877_D_690018400 AS SrvMRE_TennisFall_v1r0,
  D_564242877_D_917302906 AS SrvMRE_TennisWinter_v1r0,
  D_635874413_D_152773041 AS SrvMRE_GolfSpring_v1r0,
  D_635874413_D_249341444 AS SrvMRE_GolfSummer_v1r0,
  D_635874413_D_690018400 AS SrvMRE_GolfFall_v1r0,
  D_635874413_D_917302906 AS SrvMRE_GolfWinter_v1r0,
  D_371748514_D_152773041 AS SrvMRE_SwimLapsSpring_v1r0,
  D_371748514_D_249341444 AS SrvMRE_SwimLapsSummer_v1r0,
  D_371748514_D_690018400 AS SrvMRE_SwimLapsFall_v1r0,
  D_371748514_D_917302906 AS SrvMRE_SwimLapsWinter_v1r0,
  D_858525957_D_152773041 AS SrvMRE_BikeSpring_v1r0,
  D_858525957_D_249341444 AS SrvMRE_BikeSummer_v1r0,
  D_858525957_D_690018400 AS SrvMRE_BikeFall_v1r0,
  D_858525957_D_917302906 AS SrvMRE_BikeWinter_v1r0,
  D_787591805_D_152773041 AS SrvMRE_StrengthSpring_v1r0,
  D_787591805_D_249341444 AS SrvMRE_StrengthSummer_v1r0,
  D_787591805_D_690018400 AS SrvMRE_StrengthFall_v1r0,
  D_787591805_D_917302906 AS SrvMRE_StrengthWinter_v1r0,
  D_900299856_D_152773041 AS SrvMRE_YogaSpring_v1r0, 
  D_900299856_D_249341444 AS SrvMRE_YogaSummer_v1r0,
  D_900299856_D_690018400 AS SrvMRE_YogaFall_v1r0,
  D_900299856_D_917302906 AS SrvMRE_YogaWinter_v1r0,
  D_149884127_D_152773041 AS SrvMRE_MASpring_v1r0,
  D_149884127_D_249341444 AS SrvMRE_MASummer_v1r0,
  D_149884127_D_690018400 AS SrvMRE_MAFall_v1r0,
  D_149884127_D_917302906 AS SrvMRE_MAWinter_v1r0,
  D_845164425_D_152773041 AS SrvMRE_DanceSpring_v1r0,
  D_845164425_D_249341444 AS SrvMRE_DanceSummer_v1r0,
  D_845164425_D_690018400 AS SrvMRE_DanceFall_v1r0,
  D_845164425_D_917302906 AS SrvMRE_DanceWinter_v1r0,
  D_187772368_D_152773041 AS SrvMRE_SkiSpring_v1r0,
  D_187772368_D_249341444 AS SrvMRE_SkiSummer_v1r0,
  D_187772368_D_690018400 AS SrvMRE_SkiFall_v1r0,
  D_187772368_D_917302906 AS SrvMRE_SkiWinter_v1r0,
  D_815229596_D_152773041 AS SrvMRE_CCSkiSpring_v1r0,
  D_815229596_D_249341444 AS SrvMRE_CCSkiSummer_v1r0,
  D_815229596_D_690018400 AS SrvMRE_CCSkiFall_v1r0,
  D_815229596_D_917302906 AS SrvMRE_CCSkiWinter_v1r0,
  D_262305264_D_152773041 AS SrvMRE_SurfSpring_v1r0,
  D_262305264_D_249341444 AS SrvMRE_SurfSummer_v1r0,
  D_262305264_D_690018400 AS SrvMRE_SurfFall_v1r0,
  D_262305264_D_917302906 AS SrvMRE_SurfWinter_v1r0,
  D_409324592_D_152773041 AS SrvMRE_HICTSpring_v1r0,
  D_409324592_D_249341444 AS SrvMRE_HICTSummer_v1r0,
  D_409324592_D_690018400 AS SrvMRE_HICTFall_v1r0,
  D_409324592_D_917302906 AS SrvMRE_HICTWinter_v1r0,
  D_895837106_D_152773041 AS SrvMRE_OtherExerciseSpring_v1r0,
  D_895837106_D_249341444 AS SrvMRE_OtherExerciseSummer_v1r0,
  D_895837106_D_690018400 AS SrvMRE_OtherExerciseFall_v1r0,
  D_895837106_D_917302906 AS SrvMRE_OtherExerciseWinter_v1r0,
  1 as version
FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.module2_v1_JP`
--Remove participants that completed both v1&v2 from the table for v1
    -- Remove participants that completed both v1 & v2 from the table for v1.
    WHERE Connect_ID NOT IN ('3477605676','8065823194','4505692375','2774891615',
    '6547756854','3715901189','4394283959','8820522355','8731246565','1817817604',
    '9329247892','1996085198','1015390716','8021087753','8134860443','8166039328',
    '8016812218','1105606613','9333929469','8799687034','5671051093','1256197783','2287983457','6367118302',
    '5118827628')
)
    SELECT 
      # Select variables that are common to both versions
      dup.*,
      # Select variables that are unique to v1
      --v1.var_name,
      # Select variables that are unique to v2
      --v2.var_name,
      # Select variables from participants table
      p.d_821247024,
      p.d_747006172,
      p.d_987563196,
      p.d_536735468

    FROM
      m2_dup AS dup
    LEFT JOIN `nih-nci-dceg-connect-prod-6d04.FlatConnect.module2_v1_JP` AS v1
      ON dup.Connect_ID = v1.Connect_ID
    LEFT JOIN	`nih-nci-dceg-connect-prod-6d04.FlatConnect.module2_v2_JP` AS v2
      ON v2.Connect_ID = coalesce(dup.Connect_ID,v1.Connect_ID)
    INNER JOIN `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP` AS p
      ON coalesce(dup.Connect_ID, v1.Connect_ID, v2.Connect_ID) = p.Connect_ID
    WHERE 
      p.d_821247024 = '197316935'      -- is verified 
      AND p.d_747006172 = '104430631' -- has not withdrawn consent 
      AND p.d_987563196 = '104430631' -- should not be deceased
      AND p.d_536735468 ='231311385' -- shuld have submitted module 2
")

physical_activity_ROI <- dbGetQuery(con, sql)

#true missing flag for those who skipped this section
physical_activity_ROI <- physical_activity_ROI %>% mutate(
  true_missing = case_when(
    is.na(SrvMRE_WalkHike_v1r0)
    & is.na(SrvMRE_JogRun_v1r0)
    & is.na(SrvMRE_Tennis_v1r0)
    & is.na(SrvMRE_PlayGolf_v1r0)
    & is.na(SrvMRE_SwimLaps_v1r0)
    & is.na(SrvMRE_BikeRide_v1r0)
    & is.na(SrvMRE_Strengthening_v1r0)
    & is.na(SrvMRE_Yoga_v1r0)
    & is.na(SrvMRE_MartialArts_v1r0)
    & is.na(SrvMRE_Dance_v1r0)
    & is.na(SrvMRE_DownhillSki_v1r0)
    & is.na(SrvMRE_CrossCountry_v1r0)
    & is.na(SrvMRE_Surf_v1r0)
    & is.na(SrvMRE_HICT_v1r0)
    & is.na(SrvMRE_OtherExercise_v1r0) ~ 1, #flag for those who skipped the questions
    TRUE ~ 0 #not skipped
  ))

#flag to control optional code
run_extra_code <- FALSE

if (run_extra_code) { 
summary(freqlist(~true_missing, data = physical_activity_ROI))
    
#getting count of those who selected each activity
summary(freqlist(~SrvMRE_WalkHike_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_JogRun_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_Tennis_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_PlayGolf_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_SwimLaps_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_BikeRide_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_Strengthening_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_Yoga_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_MartialArts_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_Dance_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_DownhillSki_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_CrossCountry_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_Surf_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_HICT_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_OtherExercise_v1r0, data = physical_activity_ROI))
summary(freqlist(~SrvMRE_None_v1r0, data = physical_activity_ROI))
}

#Function to create frequency and duration variables for each activity
calculate_rec_activity <- function(data, activity_var, frequency_input, frequency_output, duration_input, duration_output) {
  data <- mutate(data,
    #Creating frequency variables for each activity
    !!frequency_output := case_when(
      !!sym(activity_var) == 1 & is.na(!!sym(frequency_input)) ~ 0,  #set to 0 if yes for activity, but missing freq response
      !!sym(activity_var) == 1 & !is.na(!!sym(frequency_input)) ~ case_when( #if yes for activity and not missing freq response
      !!sym(frequency_input) == 239152340 ~ 0.24,  #one day / 4.25 weeks per month = 0.24 days per week
      !!sym(frequency_input) == 582006876 ~ 0.59,  #averaged 2.5 days / 4.25 weeks per month = 0.59 days per week
      !!sym(frequency_input) == 645894551 ~ 1.5,   #averaged 1 to 2 days per week
      !!sym(frequency_input) == 996315715 ~ 3.5,   #averaged 3 to 4 days per week
      !!sym(frequency_input) == 671267928 ~ 5.5,   #averaged 5 to 6 days per week
      !!sym(frequency_input) == 647504893 ~ 7),    #everyday
      !!sym(activity_var) != 1 ~ 0 #if they didn't participate in activity (activity_var NE 1), frequency_output set to 0 -- this also includes those who selected none of the above or skipped the section
    ),
    #Creating duration variables for each activity
    !!duration_output := case_when(
      !!sym(activity_var) == 1 & is.na(!!sym(duration_input)) ~ 0, #set to 0 if yes for activity, but missing dur response
      !!sym(activity_var) == 1 & !is.na(!!sym(duration_input)) ~ case_when( #if yes for activity and not missing dur response
      !!sym(duration_input) == 428999623 ~ 0.125,  #averaged 7.5 minutes/60 = 0.125 hours 
      !!sym(duration_input) == 248303092 ~ 0.383,  #averaged 23 minutes/60 = 0.383 hours
      !!sym(duration_input) == 206020811 ~ 0.625,  #averaged 37.5 minutes/60 = 0.625 hours 
      !!sym(duration_input) == 264163865 ~ 0.867,  #averaged 52 minutes/60 = 0.867 hours 
      !!sym(duration_input) == 638092100 ~ 1,      #one hr 
      !!sym(duration_input) == 628177728 ~ 2,	     #two hrs
      !!sym(duration_input) == 805918496 ~ 3),		 #three hrs
      !!sym(activity_var) != 1 ~ 0 #if they didn't participate in activity (activity_var NE 1), duration_output set to 0 -- this also includes those who selected none of the above or skipped the section
    ))
 return(data)   
}
  
#Calling function that creates freq and dur variable for each activity
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI,"SrvMRE_WalkHike_v1r0","SrvMRE_WalkHikeOften_v1r0","WalkHike_freq","SrvMRE_WalkHikeTime_v1r0","WalkHike_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_JogRun_v1r0","SrvMRE_JogRunOften_v1r0","JogRun_freq","SrvMRE_JogRunTime_v1r0","JogRun_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_Tennis_v1r0","SrvMRE_TennisOften_v1r0","Tennis_freq","SrvMRE_TennisTime_v1r0","Tennis_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_PlayGolf_v1r0","SrvMRE_GolfOften_v1r0","Golf_freq","SrvMRE_GolfTime_v1r0","Golf_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_SwimLaps_v1r0","SrvMRE_SwimLapsOften_v1r0","SwimLaps_freq","SrvMRE_SwimLapsTime_v1r0","SwimLaps_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_BikeRide_v1r0","SrvMRE_BikeOften_v1r0","Bike_freq","SrvMRE_BikeTime_v1r0","Bike_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_Strengthening_v1r0","SrvMRE_StrengthOften_v1r0","Strength_freq","SrvMRE_StrengthTime_v1r0","Strength_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_Yoga_v1r0","SrvMRE_YogaOften_v1r0","Yoga_freq","SrvMRE_YogaTime_v1r0","Yoga_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_MartialArts_v1r0","SrvMRE_MAOften_v1r0","MA_freq","SrvMRE_MATime_v1r0","MA_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_Dance_v1r0","SrvMRE_DanceOften_v1r0","Dance_freq","SrvMRE_DanceTime_v1r0","Dance_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_DownhillSki_v1r0","SrvMRE_SkiOften_v1r0","Ski_freq","SrvMRE_SkiTime_v1r0","Ski_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_CrossCountry_v1r0","SrvMRE_CCSkiOften_v1r0","CCSki_freq","SrvMRE_CCSkiTime_v1r0","CCSki_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_Surf_v1r0","SrvMRE_SurfOften_v1r0","Surf_freq","SrvMRE_SurfTime_v1r0","Surf_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_HICT_v1r0","SrvMRE_HICTOften_v1r0","HICT_freq","SrvMRE_HICTTime_v1r0","HICT_dur")
physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_OtherExercise_v1r0","SrvMRE_ExerciseOften_v1r0","Exercise_freq","SrvMRE_ExerciseTime_v1r0","Exercise_dur")

#Creating variable that flags participants with inconsistent skip logic 
physical_activity_ROI_BC <- physical_activity_ROI_BC %>%
  mutate(
    inconsistent_flag = case_when(
      (SrvMRE_WalkHike_v1r0 == 0 | is.na(SrvMRE_WalkHike_v1r0)) &
        (!is.na(SrvMRE_WalkHikeOften_v1r0) | !is.na(SrvMRE_WalkHikeTime_v1r0))|
        (SrvMRE_JogRun_v1r0 == 0 | is.na(SrvMRE_JogRun_v1r0)) &
        (!is.na(SrvMRE_JogRunOften_v1r0) | !is.na(SrvMRE_JogRunTime_v1r0))|
        (SrvMRE_Tennis_v1r0 == 0 | is.na(SrvMRE_Tennis_v1r0)) &
        (!is.na(SrvMRE_TennisOften_v1r0) | !is.na(SrvMRE_TennisTime_v1r0))|
        (SrvMRE_PlayGolf_v1r0 == 0 | is.na(SrvMRE_PlayGolf_v1r0)) &
        (!is.na(SrvMRE_GolfOften_v1r0) | !is.na(SrvMRE_GolfTime_v1r0))|
        (SrvMRE_SwimLaps_v1r0 == 0 | is.na(SrvMRE_SwimLaps_v1r0)) &
        (!is.na(SrvMRE_SwimLapsOften_v1r0) | !is.na(SrvMRE_SwimLapsTime_v1r0))|
        (SrvMRE_BikeRide_v1r0 == 0 | is.na(SrvMRE_BikeRide_v1r0)) &
        (!is.na(SrvMRE_BikeOften_v1r0) | !is.na(SrvMRE_BikeTime_v1r0))|
        (SrvMRE_Strengthening_v1r0 == 0 | is.na(SrvMRE_Strengthening_v1r0)) &
        (!is.na(SrvMRE_StrengthOften_v1r0) | !is.na(SrvMRE_StrengthTime_v1r0))|
        (SrvMRE_Yoga_v1r0 == 0 | is.na(SrvMRE_Yoga_v1r0)) &
        (!is.na(SrvMRE_YogaOften_v1r0) | !is.na(SrvMRE_YogaTime_v1r0))|
        (SrvMRE_MartialArts_v1r0 == 0 | is.na(SrvMRE_MartialArts_v1r0)) &
        (!is.na(SrvMRE_MAOften_v1r0) | !is.na(SrvMRE_MATime_v1r0))|
        (SrvMRE_Dance_v1r0 == 0 | is.na(SrvMRE_Dance_v1r0)) &
        (!is.na(SrvMRE_DanceOften_v1r0) | !is.na(SrvMRE_DanceTime_v1r0))|
        (SrvMRE_DownhillSki_v1r0 == 0 | is.na(SrvMRE_DownhillSki_v1r0)) &
        (!is.na(SrvMRE_SkiOften_v1r0) | !is.na(SrvMRE_SkiTime_v1r0))|
        (SrvMRE_CrossCountry_v1r0 == 0 | is.na(SrvMRE_CrossCountry_v1r0)) &
        (!is.na(SrvMRE_CCSkiOften_v1r0) | !is.na(SrvMRE_CCSkiTime_v1r0))|
        (SrvMRE_Surf_v1r0 == 0 | is.na(SrvMRE_Surf_v1r0)) &
        (!is.na(SrvMRE_SurfOften_v1r0) | !is.na(SrvMRE_SurfTime_v1r0))|
        (SrvMRE_HICT_v1r0 == 0 | is.na(SrvMRE_HICT_v1r0)) &
        (!is.na(SrvMRE_HICTOften_v1r0) | !is.na(SrvMRE_HICTTime_v1r0))|
        (SrvMRE_OtherExercise_v1r0 == 0 | is.na(SrvMRE_OtherExercise_v1r0)) &
        (!is.na(SrvMRE_ExerciseOften_v1r0) | !is.na(SrvMRE_ExerciseTime_v1r0)) ~ 1,
      TRUE ~ 0
    ))

if (run_extra_code) { 
#checking above flag
sample_walkhike_check <- physical_activity_ROI_BC %>% 
  select(Connect_ID, inconsistent_flag, SrvMRE_WalkHike_v1r0, SrvMRE_WalkHikeOften_v1r0, SrvMRE_WalkHikeTime_v1r0) %>%
 filter (inconsistent_flag == 1 & (SrvMRE_WalkHike_v1r0 == 0 | is.na(SrvMRE_WalkHike_v1r0)))
 
#Checking to see if any values were assigned incorrectly for frequency and duration
qc_activity_freq <- function(data, base_var, input_var, output_var) {
  data %>% 
    filter((!!sym(base_var) != 1 & !!sym(output_var) != 0) |
           (!!sym(base_var) == 1 & !is.na(!!sym(input_var)))) %>%
    summarize(
      total_mismatches = sum(
        (!!sym(input_var) == 239152340 & !!sym(output_var) != 0.24) |
        (!!sym(input_var) == 582006876 & !!sym(output_var) != 0.59) |
        (!!sym(input_var) == 645894551 & !!sym(output_var) != 1.5) |
        (!!sym(input_var) == 996315715 & !!sym(output_var) != 3.5) |
        (!!sym(input_var) == 671267928 & !!sym(output_var) != 5.5) |
        (!!sym(input_var) == 647504893 & !!sym(output_var) != 7),
        na.rm = TRUE),
      total_checked = n())
}

qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_WalkHike_v1r0", "SrvMRE_WalkHikeOften_v1r0", "WalkHike_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_JogRun_v1r0", "SrvMRE_JogRunOften_v1r0", "JogRun_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_Tennis_v1r0", "SrvMRE_TennisOften_v1r0", "Tennis_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_PlayGolf_v1r0", "SrvMRE_GolfOften_v1r0", "Golf_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_SwimLaps_v1r0", "SrvMRE_SwimLapsOften_v1r0", "SwimLaps_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_BikeRide_v1r0", "SrvMRE_BikeOften_v1r0", "Bike_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_Strengthening_v1r0", "SrvMRE_StrengthOften_v1r0", "Strength_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_Yoga_v1r0", "SrvMRE_YogaOften_v1r0", "Yoga_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_MartialArts_v1r0", "SrvMRE_MAOften_v1r0", "MA_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_Dance_v1r0", "SrvMRE_DanceOften_v1r0", "Dance_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_DownhillSki_v1r0", "SrvMRE_SkiOften_v1r0", "Ski_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_CrossCountry_v1r0", "SrvMRE_CCSkiOften_v1r0", "CCSki_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_Surf_v1r0", "SrvMRE_SurfOften_v1r0", "Surf_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_HICT_v1r0", "SrvMRE_HICTOften_v1r0", "HICT_freq")
qc_activity_freq(physical_activity_ROI_BC, "SrvMRE_OtherExercise_v1r0", "SrvMRE_ExerciseOften_v1r0", "Exercise_freq")                  

qc_activity_duration <- function(data, base_var, input_var, output_var) {
  data %>% 
    filter((!!sym(base_var) != 1 & !!sym(output_var) != 0) |
             (!!sym(base_var) == 1 & !is.na(!!sym(input_var)))) %>%
    summarize(
      total_mismatches = sum(
        (!!sym(input_var) == 428999623 & !!sym(output_var) != 0.125) |
          (!!sym(input_var) == 248303092 & !!sym(output_var) != 0.383) |
          (!!sym(input_var) == 206020811 & !!sym(output_var) != 0.625) |
          (!!sym(input_var) == 264163865 & !!sym(output_var) != 0.867) |
          (!!sym(input_var) == 638092100 & !!sym(output_var) != 1) |
          (!!sym(input_var) == 628177728 & !!sym(output_var) != 2) |
          (!!sym(input_var) == 805918496 & !!sym(output_var) != 3),  
        na.rm = TRUE),
      total_checked = n())
}

qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_WalkHike_v1r0", "SrvMRE_WalkHikeTime_v1r0", "WalkHike_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_JogRun_v1r0", "SrvMRE_JogRunTime_v1r0", "JogRun_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_Tennis_v1r0", "SrvMRE_TennisTime_v1r0", "Tennis_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_PlayGolf_v1r0", "SrvMRE_GolfTime_v1r0", "Golf_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_SwimLaps_v1r0", "SrvMRE_SwimLapsTime_v1r0", "SwimLaps_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_BikeRide_v1r0", "SrvMRE_BikeTime_v1r0", "Bike_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_Strengthening_v1r0", "SrvMRE_StrengthTime_v1r0", "Strength_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_Yoga_v1r0", "SrvMRE_YogaTime_v1r0", "Yoga_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_MartialArts_v1r0", "SrvMRE_MATime_v1r0", "MA_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_Dance_v1r0", "SrvMRE_DanceTime_v1r0", "Dance_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_DownhillSki_v1r0", "SrvMRE_SkiTime_v1r0", "Ski_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_CrossCountry_v1r0", "SrvMRE_CCSkiTime_v1r0", "CCSki_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_Surf_v1r0", "SrvMRE_SurfTime_v1r0", "Surf_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_HICT_v1r0", "SrvMRE_HICTTime_v1r0", "HICT_dur")
qc_activity_duration(physical_activity_ROI_BC, "SrvMRE_OtherExercise_v1r0", "SrvMRE_ExerciseTime_v1r0", "Exercise_dur")      
}

##Function to create seasonality variables, ensuring all season variables are numeric
calculate_season <- function(data, activity_var, spring_input, summer_input, fall_input, winter_input, season_output) {
  data <- mutate(data,
   !!season_output := case_when(
    !!sym(activity_var) == 1 & rowSums(cbind(
      as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))), 
        na.rm = TRUE) == 1 ~ 0.25, # One season selected
   !!sym(activity_var) == 1 & rowSums(cbind(
    as.numeric(!!sym(spring_input)),as.numeric(!!sym(summer_input)),as.numeric(!!sym(fall_input)),as.numeric(!!sym(winter_input))),
      na.rm = TRUE) == 2 ~ 0.50, # Two seasons selected
   !!sym(activity_var) == 1 & rowSums(cbind(
    as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)),as.numeric(!!sym(winter_input))), 
      na.rm = TRUE) == 3 ~ 0.75, # Three seasons selected
   !!sym(activity_var) == 1 & rowSums(cbind(
    as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))), 
      na.rm = TRUE) == 4 ~ 1.00, # Four seasons selected
   !!sym(activity_var) == 1 & rowSums(cbind(
    as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))), 
      na.rm = TRUE) == 0 ~ 0, # If none selected, set to 0
   !!sym(activity_var) != 1 ~ 0 )) # If activity not selected, set to 0 -- this also includes those who selected none of the above or skipped the section
  return(data)
  }

#Calling function that creates seasonality variables 
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_WalkHike_v1r0", "SrvMRE_WalkHikeSpring_v1r0", "SrvMRE_WalkHikeSummer_v1r0", "SrvMRE_WalkHikeFall_v1r0", "SrvMRE_WalkHikeWinter_v1r0","WalkHike_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_JogRun_v1r0", "SrvMRE_JogRunSpring_v1r0", "SrvMRE_JogRunSummer_v1r0", "SrvMRE_JogRunFall_v1r0", "SrvMRE_JogRunWinter_v1r0", "JogRun_season")                              
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_Tennis_v1r0", "SrvMRE_TennisSpring_v1r0", "SrvMRE_TennisSummer_v1r0", "SrvMRE_TennisFall_v1r0", "SrvMRE_TennisWinter_v1r0", "Tennis_season")                                 
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_PlayGolf_v1r0", "SrvMRE_GolfSpring_v1r0", "SrvMRE_GolfSummer_v1r0", "SrvMRE_GolfFall_v1r0", "SrvMRE_GolfWinter_v1r0", "Golf_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_SwimLaps_v1r0", "SrvMRE_SwimLapsSpring_v1r0", "SrvMRE_SwimLapsSummer_v1r0", "SrvMRE_SwimLapsFall_v1r0", "SrvMRE_SwimLapsWinter_v1r0", "SwimLaps_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_BikeRide_v1r0", "SrvMRE_BikeSpring_v1r0", "SrvMRE_BikeSummer_v1r0", "SrvMRE_BikeFall_v1r0", "SrvMRE_BikeWinter_v1r0", "Bike_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_Strengthening_v1r0", "SrvMRE_StrengthSpring_v1r0", "SrvMRE_StrengthSummer_v1r0", "SrvMRE_StrengthFall_v1r0", "SrvMRE_StrengthWinter_v1r0", "Strength_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_Yoga_v1r0", "SrvMRE_YogaSpring_v1r0", "SrvMRE_YogaSummer_v1r0", "SrvMRE_YogaFall_v1r0", "SrvMRE_YogaWinter_v1r0", "Yoga_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_MartialArts_v1r0", "SrvMRE_MASpring_v1r0", "SrvMRE_MASummer_v1r0", "SrvMRE_MAFall_v1r0", "SrvMRE_MAWinter_v1r0", "MA_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_Dance_v1r0", "SrvMRE_DanceSpring_v1r0", "SrvMRE_DanceSummer_v1r0", "SrvMRE_DanceFall_v1r0", "SrvMRE_DanceWinter_v1r0", "Dance_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_DownhillSki_v1r0", "SrvMRE_SkiSpring_v1r0", "SrvMRE_SkiSummer_v1r0", "SrvMRE_SkiFall_v1r0", "SrvMRE_SkiWinter_v1r0", "Ski_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_CrossCountry_v1r0", "SrvMRE_CCSkiSpring_v1r0", "SrvMRE_CCSkiSummer_v1r0", "SrvMRE_CCSkiFall_v1r0", "SrvMRE_CCSkiWinter_v1r0", "CCSki_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_Surf_v1r0", "SrvMRE_SurfSpring_v1r0", "SrvMRE_SurfSummer_v1r0", "SrvMRE_SurfFall_v1r0", "SrvMRE_SurfWinter_v1r0", "Surf_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_HICT_v1r0", "SrvMRE_HICTSpring_v1r0", "SrvMRE_HICTSummer_v1r0", "SrvMRE_HICTFall_v1r0", "SrvMRE_HICTWinter_v1r0", "HICT_season")
physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC,"SrvMRE_OtherExercise_v1r0", "SrvMRE_OtherExerciseSpring_v1r0", "SrvMRE_OtherExerciseSummer_v1r0", "SrvMRE_OtherExerciseFall_v1r0", "SrvMRE_OtherExerciseWinter_v1r0", "Exercise_season")

if (run_extra_code) { 
#sampling rows to check above calculations
sample_JogRun_season <- physical_activity_ROI_BC %>%
  select(Connect_ID, SrvMRE_JogRunSpring_v1r0, SrvMRE_JogRunSummer_v1r0, SrvMRE_JogRunFall_v1r0, SrvMRE_JogRunWinter_v1r0, JogRun_season) %>%
  sample_n(100)
sample_Bike_season <- physical_activity_ROI_BC %>%
  select(Connect_ID, SrvMRE_BikeSpring_v1r0, SrvMRE_BikeSummer_v1r0, SrvMRE_BikeFall_v1r0, SrvMRE_BikeWinter_v1r0, Bike_season) %>%
  sample_n(100)
}

#Calculating hours per week for each activity per Hayden's example --> jog_hrweek = jog_freq*jog_duration*jog_season
physical_activity_ROI_BC$WalkHike_hrweek = physical_activity_ROI_BC$WalkHike_freq * physical_activity_ROI_BC$WalkHike_dur * physical_activity_ROI_BC$WalkHike_season
physical_activity_ROI_BC$JogRun_hrweek = physical_activity_ROI_BC$JogRun_freq * physical_activity_ROI_BC$JogRun_dur * physical_activity_ROI_BC$JogRun_season
physical_activity_ROI_BC$Tennis_hrweek = physical_activity_ROI_BC$Tennis_freq * physical_activity_ROI_BC$Tennis_dur * physical_activity_ROI_BC$Tennis_season
physical_activity_ROI_BC$Golf_hrweek = physical_activity_ROI_BC$Golf_freq * physical_activity_ROI_BC$Golf_dur * physical_activity_ROI_BC$Golf_season
physical_activity_ROI_BC$SwimLaps_hrweek = physical_activity_ROI_BC$SwimLaps_freq * physical_activity_ROI_BC$SwimLaps_dur * physical_activity_ROI_BC$SwimLaps_season
physical_activity_ROI_BC$Bike_hrweek = physical_activity_ROI_BC$Bike_freq * physical_activity_ROI_BC$Bike_dur * physical_activity_ROI_BC$Bike_season
physical_activity_ROI_BC$Strength_hrweek = physical_activity_ROI_BC$Strength_freq * physical_activity_ROI_BC$Strength_dur * physical_activity_ROI_BC$Strength_season
physical_activity_ROI_BC$Yoga_hrweek = physical_activity_ROI_BC$Yoga_freq * physical_activity_ROI_BC$Yoga_dur * physical_activity_ROI_BC$Yoga_season
physical_activity_ROI_BC$MA_hrweek = physical_activity_ROI_BC$MA_freq * physical_activity_ROI_BC$MA_dur * physical_activity_ROI_BC$MA_season
physical_activity_ROI_BC$Dance_hrweek = physical_activity_ROI_BC$Dance_freq * physical_activity_ROI_BC$Dance_dur * physical_activity_ROI_BC$Dance_season
physical_activity_ROI_BC$Ski_hrweek = physical_activity_ROI_BC$Ski_freq * physical_activity_ROI_BC$Ski_dur * physical_activity_ROI_BC$Ski_season
physical_activity_ROI_BC$CCSki_hrweek = physical_activity_ROI_BC$CCSki_freq * physical_activity_ROI_BC$CCSki_dur * physical_activity_ROI_BC$CCSki_season
physical_activity_ROI_BC$Surf_hrweek = physical_activity_ROI_BC$Surf_freq * physical_activity_ROI_BC$Surf_dur * physical_activity_ROI_BC$Surf_season
physical_activity_ROI_BC$HICT_hrweek = physical_activity_ROI_BC$HICT_freq * physical_activity_ROI_BC$HICT_dur * physical_activity_ROI_BC$HICT_season
physical_activity_ROI_BC$Exercise_hrweek = physical_activity_ROI_BC$Exercise_freq * physical_activity_ROI_BC$Exercise_dur * physical_activity_ROI_BC$Exercise_season

if (run_extra_code) { 
#sampling rows to check above calculations
sample_WalkHike_hrweek <- physical_activity_ROI_BC %>%
  select(Connect_ID, WalkHike_hrweek, WalkHike_freq, WalkHike_dur, WalkHike_season) %>%
  sample_n(25)
sample_Yoga_hrweek <- physical_activity_ROI_BC %>%
  select(Connect_ID, Yoga_hrweek, Yoga_freq, Yoga_dur, Yoga_season) %>%
  sample_n(25)
}

#Calculating MET hrs for each activity
physical_activity_ROI_BC$WalkHike_METhr = physical_activity_ROI_BC$WalkHike_hrweek * 4.8
physical_activity_ROI_BC$JogRun_METhr = physical_activity_ROI_BC$JogRun_hrweek * 7.5
physical_activity_ROI_BC$Tennis_METhr = physical_activity_ROI_BC$Tennis_hrweek * 6.8
physical_activity_ROI_BC$Golf_METhr = physical_activity_ROI_BC$Golf_hrweek * 4.5
physical_activity_ROI_BC$SwimLaps_METhr = physical_activity_ROI_BC$SwimLaps_hrweek * 5.8
physical_activity_ROI_BC$Bike_METhr = physical_activity_ROI_BC$Bike_hrweek * 7.0
physical_activity_ROI_BC$Strength_METhr = physical_activity_ROI_BC$Strength_hrweek * 3.5
physical_activity_ROI_BC$Yoga_METhr = physical_activity_ROI_BC$Yoga_hrweek * 3.3
physical_activity_ROI_BC$MA_METhr = physical_activity_ROI_BC$MA_hrweek * 5.3
physical_activity_ROI_BC$Dance_METhr = physical_activity_ROI_BC$Dance_hrweek * 4.8
physical_activity_ROI_BC$Ski_METhr = physical_activity_ROI_BC$Ski_hrweek * 6.3
physical_activity_ROI_BC$CCSki_METhr = physical_activity_ROI_BC$CCSki_hrweek * 8.5
physical_activity_ROI_BC$Surf_METhr = physical_activity_ROI_BC$Surf_hrweek * 3.0
physical_activity_ROI_BC$HICT_METhr = physical_activity_ROI_BC$HICT_hrweek * 7.5
physical_activity_ROI_BC$Exercise_METhr = physical_activity_ROI_BC$Exercise_hrweek * 3.8

if (run_extra_code) { 
#sampling 10 rows to check above calculations
sample_WalkHike_METhr <- physical_activity_ROI_BC %>%
  select(Connect_ID, WalkHike_hrweek, WalkHike_METhr) %>%
  sample_n(10)
}

#Creating a single overall energy expenditure (METhr) score by summing the METhr variables across all activities
#na.rm = TRUE to allow for calculation of total energy expenditure score even when input METhr values are missing, most won't participate in all activities 
physical_activity_ROI_BC$overall_METhr <- rowSums(
  physical_activity_ROI_BC[, c("WalkHike_METhr", "JogRun_METhr", "Tennis_METhr", "Golf_METhr", "SwimLaps_METhr", "Bike_METhr", 
    "Strength_METhr", "Yoga_METhr", "MA_METhr", "Dance_METhr", "Ski_METhr", "CCSki_METhr", "Surf_METhr", 
    "HICT_METhr", "Exercise_METhr")], na.rm = TRUE)

#Calculating the total duration of recreation/exercise per week by summing the hours/week variables across all activities
#na.rm = TRUE to allow for calculation of total energy expenditure score even when input hrs/week values are missing, most won't participate in all activities
physical_activity_ROI_BC$overall_hrweek <- rowSums(
  physical_activity_ROI_BC[, c("WalkHike_hrweek", "JogRun_hrweek", "Tennis_hrweek", "Golf_hrweek", "SwimLaps_hrweek", 
    "Bike_hrweek", "Strength_hrweek", "Yoga_hrweek", "MA_hrweek", "Dance_hrweek", "Ski_hrweek", "CCSki_hrweek", "Surf_hrweek", 
    "HICT_hrweek", "Exercise_hrweek")], na.rm = TRUE)

if (run_extra_code) { 
#Checking overall METhr and hours per week variables
sample_overall_METhr <- physical_activity_ROI_BC %>%
  select(Connect_ID, overall_METhr, WalkHike_METhr, JogRun_METhr, Tennis_METhr, Golf_METhr, SwimLaps_METhr, Bike_METhr, Strength_METhr, Yoga_METhr, MA_METhr, 
         Dance_METhr, Ski_METhr, CCSki_METhr, Surf_METhr, HICT_METhr, Exercise_METhr) %>%
  sample_n(10)
sample_overall_hrweek <- physical_activity_ROI_BC %>%
  select(Connect_ID, overall_hrweek, WalkHike_hrweek, JogRun_hrweek, Tennis_hrweek, Golf_hrweek, SwimLaps_hrweek, Bike_hrweek, Strength_hrweek, Yoga_hrweek, MA_hrweek, 
         Dance_hrweek, Ski_hrweek, CCSki_hrweek, Surf_hrweek, HICT_hrweek, Exercise_hrweek) %>%
  sample_n(10)
}

#Based on the ‘Connect PAQ Coding METs July 2024 .xlsx’ document:
#Light = < 3.0 MET, Moderate = 3.0 – 5.9 MET, Vigorous = 6.0+ MET

#Calculating hours per week and METhr variables for each intensity category, ignoring potential missing inputs to calculate final variable
#None of the recreational activities fall into the light intensity category
physical_activity_ROI_BC <- physical_activity_ROI_BC %>% mutate(
  moderate_hrweek = rowSums(cbind(WalkHike_hrweek, Golf_hrweek, SwimLaps_METhr, Strength_hrweek, Yoga_hrweek, MA_hrweek, Dance_hrweek, Surf_hrweek,  Exercise_hrweek), na.rm = TRUE),
  moderate_METhr = rowSums(cbind(WalkHike_METhr, Golf_METhr, SwimLaps_METhr, Strength_METhr, Yoga_METhr, MA_METhr, Dance_METhr, Surf_METhr,  Exercise_METhr), na.rm = TRUE),
  vigorous_hrweek = rowSums(cbind(JogRun_hrweek, Tennis_hrweek, Bike_hrweek, Ski_hrweek, CCSki_hrweek, HICT_hrweek), na.rm = TRUE),
  vigorous_METhr = rowSums(cbind(JogRun_METhr, Tennis_METhr, Bike_METhr, Ski_METhr, CCSki_METhr, HICT_METhr), na.rm = TRUE))

#Creating additional hours per week and MET hours per week variables without strengthening activity included
#Vigorous activity variables remain the same because they don't include strengthening activities
physical_activity_ROI_BC <- physical_activity_ROI_BC %>% mutate(
  moderate_aerobic_hrweek = rowSums(cbind(WalkHike_hrweek, Golf_hrweek, SwimLaps_METhr, Yoga_hrweek, MA_hrweek, Dance_hrweek, Surf_hrweek,  Exercise_hrweek), na.rm = TRUE),
  moderate_aerobic_METhr = rowSums(cbind(WalkHike_METhr, Golf_METhr, SwimLaps_METhr, Yoga_METhr, MA_METhr, Dance_METhr, Surf_METhr,  Exercise_METhr), na.rm = TRUE))

#Creating minutes per week variable for moderate and vigorous intensity
physical_activity_ROI_BC <- physical_activity_ROI_BC %>% mutate(
  moderate_mins_week_aerobic = moderate_aerobic_hrweek * 60,
  vigorous_mins_week = vigorous_hrweek * 60,
  #Harmonizing moderate and vigorous activity to create single 'guideline minutes' variable
  guideline_minutes = moderate_mins_week_aerobic + (vigorous_mins_week*2)) 

#Creating flag for guideline minutes, where <150 mins=not meeting, 150-300 mins=meeting, and >300 mins=exceeding
physical_activity_ROI_BC <- physical_activity_ROI_BC %>% mutate(
  guideline_cat = case_when(
  guideline_minutes < 150 ~ "not meeting",
  guideline_minutes >= 150 & guideline_minutes <= 300 ~ "meeting",
  guideline_minutes > 300 ~ "exceeding"
  ))

#Y/N measure for muscle strengthening
physical_activity_ROI_BC <- physical_activity_ROI_BC %>% mutate(
  strengthening_binary = case_when(SrvMRE_Strengthening_v1r0 == 1 ~ "Yes",
  SrvMRE_Strengthening_v1r0 == 0 | is.na(SrvMRE_Strengthening_v1r0) ~ "No"))

#selecting final dataset 
physical_activity_ROI_final <- physical_activity_ROI_BC %>%
  select(Connect_ID, guideline_cat, inconsistent_flag, true_missing, strengthening_binary)

if (run_extra_code) { 
#############Summary statistics including participants with 0 guideline minutes
quantile(physical_activity_ROI_BC$guideline_minutes, probs = c(0.25, 0.50, 0.75, 0.90, 0.95, 0.99), na.rm = TRUE)
mean(physical_activity_ROI_BC$guideline_minutes)
min(physical_activity_ROI_BC$guideline_minutes)
max(physical_activity_ROI_BC$guideline_minutes)

#Counting participants with 0 guideline minutes per week
physical_activity_ROI_BC %>% summarise(Guideline_Mins_Zero_Count = sum(guideline_minutes == 0, na.rm = TRUE))

#Summary statistics excluding participants with 0 guideline minutes
filtered_non_zeroes <- physical_activity_ROI_BC %>% filter(guideline_minutes > 0)
quantile(filtered_non_zeroes$guideline_minutes, probs = c(0.25, 0.50, 0.75, 0.90, 0.95, 0.99), na.rm = TRUE)
mean(filtered_non_zeroes$guideline_minutes)
min(filtered_non_zeroes$guideline_minutes)
max(filtered_non_zeroes$guideline_minutes)

#Counts per guideline minutes category
summary(freqlist(~guideline_cat, data = physical_activity_ROI_BC))

#Cross tab for strengthening variable
table(physical_activity_ROI_BC$SrvMRE_Strengthening_v1r0, physical_activity_ROI_BC$strengthening_binary, useNA="ifany")
summary(freqlist(~strengthening_binary, data = physical_activity_ROI_BC))
summary(freqlist(~strengthening_guideline, data = muscle_strengthening))

#Calculating outliers
#closer look at those who have guideline minutes > 99th percentile
filtered_extreme <- physical_activity_ROI_BC %>% filter(guideline_minutes >= 1890.00) %>% 
  select(Connect_ID, guideline_minutes, moderate_mins_week_aerobic, vigorous_mins_week,
         SrvMRE_WalkHike_v1r0, SrvMRE_WalkHikeOften_v1r0, WalkHike_freq, SrvMRE_WalkHikeTime_v1r0, WalkHike_dur, SrvMRE_WalkHikeSpring_v1r0, SrvMRE_WalkHikeSummer_v1r0, SrvMRE_WalkHikeFall_v1r0, SrvMRE_WalkHikeWinter_v1r0, WalkHike_season,
         SrvMRE_JogRun_v1r0, SrvMRE_JogRunOften_v1r0, JogRun_freq, SrvMRE_JogRunTime_v1r0, JogRun_dur, SrvMRE_JogRunSpring_v1r0, SrvMRE_JogRunSummer_v1r0, SrvMRE_JogRunFall_v1r0, SrvMRE_JogRunWinter_v1r0, JogRun_season,                              
         SrvMRE_Tennis_v1r0, SrvMRE_TennisOften_v1r0, Tennis_freq, SrvMRE_TennisTime_v1r0, Tennis_dur, SrvMRE_TennisSpring_v1r0, SrvMRE_TennisSummer_v1r0, SrvMRE_TennisFall_v1r0, SrvMRE_TennisWinter_v1r0, Tennis_season,
         SrvMRE_PlayGolf_v1r0, SrvMRE_GolfOften_v1r0, Golf_freq, SrvMRE_GolfTime_v1r0, Golf_dur, SrvMRE_GolfSpring_v1r0, SrvMRE_GolfSummer_v1r0, SrvMRE_GolfFall_v1r0, SrvMRE_GolfWinter_v1r0, Golf_season,
         SrvMRE_SwimLaps_v1r0, SrvMRE_SwimLapsOften_v1r0, SwimLaps_freq, SrvMRE_SwimLapsTime_v1r0, SwimLaps_dur, SrvMRE_SwimLapsSpring_v1r0, SrvMRE_SwimLapsSummer_v1r0, SrvMRE_SwimLapsFall_v1r0, SrvMRE_SwimLapsWinter_v1r0, SwimLaps_season, 
         SrvMRE_BikeRide_v1r0, SrvMRE_BikeOften_v1r0, Bike_freq, SrvMRE_BikeTime_v1r0, Bike_dur, SrvMRE_BikeSpring_v1r0, SrvMRE_BikeSummer_v1r0, SrvMRE_BikeFall_v1r0, SrvMRE_BikeWinter_v1r0, Bike_season,  
         SrvMRE_Yoga_v1r0, SrvMRE_YogaOften_v1r0, Yoga_freq, SrvMRE_YogaTime_v1r0, Yoga_dur, SrvMRE_YogaSpring_v1r0, SrvMRE_YogaSummer_v1r0, SrvMRE_YogaFall_v1r0, SrvMRE_YogaWinter_v1r0, Yoga_season,  
         SrvMRE_MartialArts_v1r0, SrvMRE_MAOften_v1r0, MA_freq, SrvMRE_MATime_v1r0, MA_dur, SrvMRE_MASpring_v1r0, SrvMRE_MASummer_v1r0, SrvMRE_MAFall_v1r0, SrvMRE_MAWinter_v1r0, MA_season,  
         SrvMRE_Dance_v1r0, SrvMRE_DanceOften_v1r0, Dance_freq, SrvMRE_DanceTime_v1r0, Dance_dur, SrvMRE_DanceSpring_v1r0, SrvMRE_DanceSummer_v1r0, SrvMRE_DanceFall_v1r0, SrvMRE_DanceWinter_v1r0, Dance_season,  
         SrvMRE_DownhillSki_v1r0, SrvMRE_SkiOften_v1r0, Ski_freq, SrvMRE_SkiTime_v1r0, Ski_dur, SrvMRE_SkiSpring_v1r0, SrvMRE_SkiSummer_v1r0, SrvMRE_SkiFall_v1r0, SrvMRE_SkiWinter_v1r0, Ski_season, 
         SrvMRE_CrossCountry_v1r0, SrvMRE_CCSkiOften_v1r0, CCSki_freq, SrvMRE_CCSkiTime_v1r0, CCSki_dur, SrvMRE_CCSkiSpring_v1r0, SrvMRE_CCSkiSummer_v1r0, SrvMRE_CCSkiFall_v1r0, SrvMRE_CCSkiWinter_v1r0, CCSki_season, 
         SrvMRE_Surf_v1r0, SrvMRE_SurfOften_v1r0, Surf_freq, SrvMRE_SurfTime_v1r0, Surf_dur, SrvMRE_SurfSpring_v1r0, SrvMRE_SurfSummer_v1r0, SrvMRE_SurfFall_v1r0, SrvMRE_SurfWinter_v1r0, Surf_season, 
         SrvMRE_HICT_v1r0, SrvMRE_HICTOften_v1r0, HICT_freq, SrvMRE_HICTTime_v1r0, HICT_dur, SrvMRE_HICTSpring_v1r0, SrvMRE_HICTSummer_v1r0, SrvMRE_HICTFall_v1r0, SrvMRE_HICTWinter_v1r0, HICT_season,  
         SrvMRE_OtherExercise_v1r0, SrvMRE_ExerciseOften_v1r0, Exercise_freq, SrvMRE_ExerciseTime_v1r0, Exercise_dur, SrvMRE_OtherExerciseSpring_v1r0, SrvMRE_OtherExerciseSummer_v1r0, SrvMRE_OtherExerciseFall_v1r0, SrvMRE_OtherExerciseWinter_v1r0, Exercise_season)

write_csv(filtered_extreme, "/Users/crawfordbm/Documents/PhysicalActivityROI_ExtremeMinutes.csv")

extreme_walk <- filtered_extreme %>% 
  filter(WalkHike_freq >= 5.50 & filtered_extreme$WalkHike_dur >= 2.000)
extreme_jog <- filtered_extreme %>% 
  filter(JogRun_freq >= 5.50 & filtered_extreme$JogRun_dur >= 2.000)
extreme_jog <- filtered_extreme %>% 
  filter(Tennis_freq >= 5.50 & filtered_extreme$Tennis_dur >= 2.000)
extreme_golf <- filtered_extreme %>% 
  filter(Golf_freq >= 5.50 & filtered_extreme$Golf_dur >= 2.000)
extreme_swim <- filtered_extreme %>% 
  filter(SwimLaps_freq >= 5.50 & filtered_extreme$SwimLaps_dur >= 2.000)
extreme_bike <- filtered_extreme %>% 
  filter(Bike_freq >= 5.50 & filtered_extreme$Bike_dur >= 2.000)
extreme_jog <- filtered_extreme %>% 
  filter(Yoga_freq >= 5.50 & filtered_extreme$Yoga_dur >= 2.000)
extreme_golf <- filtered_extreme %>% 
  filter(MA_freq >= 5.50 & filtered_extreme$MA_dur >= 2.000)
extreme_swim <- filtered_extreme %>% 
  filter(Dance_freq >= 5.50 & filtered_extreme$Dance_dur >= 2.000)
extreme_ski <- filtered_extreme %>% 
  filter(Ski_freq >= 5.50 & filtered_extreme$Ski_dur >= 2.000)
extreme_ccski <- filtered_extreme %>% 
  filter(CCSki_freq >= 5.50 & filtered_extreme$CCSki_dur >= 2.000)
extreme_surf <- filtered_extreme %>% 
  filter(Surf_freq >= 5.50 & filtered_extreme$Surf_dur >= 2.000)
extreme_HICT <- filtered_extreme %>% 
  filter(HICT_freq >= 5.50 & filtered_extreme$HICT_dur >= 2.000)
extreme_other <- filtered_extreme %>% 
  filter(Exercise_freq >= 5.50 & filtered_extreme$Exercise_dur >= 2.000)

##pulling in module 1 data for stratified descriptive statistics
recr_M1 <- bq_project_query(project, query="SELECT token,Connect_ID, d_821247024, d_914594314,  d_827220437,d_512820379,
    d_949302066 , d_517311251  FROM  `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP` WHERE  d_821247024 = '197316935'     
    AND d_747006172 = '104430631'
    AND d_987563196 = '104430631'
    AND d_536735468 ='231311385' 
")
recr_m1 <- bq_table_download(recr_M1,bigint = "integer64")
cnames <- names(recr_m1)
# Check that it doesn't match any non-number
numbers_only <- function(x) !grepl("\\D", x)
# to check variables in recr_noinact_wl1
for (i in 1: length(cnames)){
  varname <- cnames[i]
  var<-pull(recr_m1,varname)
  recr_m1[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
}
sql_M1_1 <- bq_project_query(project, query="SELECT * FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.module1_v1_JP` where Connect_ID is not null")
sql_M1_2 <- bq_project_query(project, query="SELECT * FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.module1_v2_JP` where Connect_ID is not null")

M1_V1 <- bq_table_download(sql_M1_1,bigint = "integer64") 
M1_V2 <- bq_table_download(sql_M1_2,bigint = "integer64") 

# Select matching column names
M1_V1_vars <- colnames(M1_V1)
M1_V2_vars <- colnames(M1_V2)
common_vars <- intersect(M1_V1_vars, M1_V2_vars)

# Subset to common columns
M1_V1_common <- M1_V1[, common_vars]
M1_V2_common <- M1_V2[, common_vars]

# Add version indicator
M1_V1_common$version <- 1
M1_V2_common$version <- 2

# Identify columns with mismatched types
mismatched_cols <- names(M1_V1_common)[sapply(names(M1_V1_common), function(col) {
  class(M1_V1_common[[col]]) != class(M1_V2_common[[col]])
})]

# Convert mismatched columns to character for consistency
M1_V1_common <- M1_V1_common %>%
  mutate(across(all_of(mismatched_cols), as.character))
M1_V2_common <- M1_V2_common %>%
  mutate(across(all_of(mismatched_cols), as.character))

# Combine both versions for participants who completed both
M1_common <- bind_rows(M1_V1_common, M1_V2_common) %>%
  arrange(Connect_ID, desc(version))

# For columns unique to each version
V1_only_vars <- setdiff(M1_V1_vars, common_vars)
V2_only_vars <- setdiff(M1_V2_vars, common_vars)

# Subset each version for unique columns and add version indicator
m1_v1_only <- M1_V1[, c("Connect_ID", V1_only_vars)] %>%
  mutate(version = 1)
m1_v2_only <- M1_V2[, c("Connect_ID", V2_only_vars)] %>%
  mutate(version = 2)

# Combine the unique and common data
m1_common_v1 <- left_join(M1_common, m1_v1_only, by = c("Connect_ID", "version"))
m1_combined_v1v2 <- left_join(m1_common_v1, m1_v2_only, by = c("Connect_ID", "version"))

# Filter for complete cases where specific completion criteria are met
m1_complete <- m1_combined_v1v2 %>%
  filter(Connect_ID %in% recr_m1$Connect_ID[recr_m1$d_949302066 == 231311385]) %>%
  arrange(desc(version))

# Remove duplicates, keeping only the most recent version for each Connect_ID
m1_complete_nodup <- m1_complete[!duplicated(m1_complete$Connect_ID),]

m1_complete_nodup$Connect_ID <- as.numeric(m1_complete_nodup$Connect_ID)

### Define requirements of the data: only including those with connect_id ne null, will apply additional criteria when merging with module 2 data
parts <- "SELECT Connect_ID, token, D_512820379, D_471593703, state_d_934298480, D_230663853,
D_335767902, D_982402227, D_919254129, D_699625233, D_564964481, D_795827569, D_544150384,
D_371067537, D_430551721, D_821247024, D_914594314,  state_d_725929722, d_827220437, 
D_949302066 , D_517311251, D_205553981, D_117249500, d_430551721, d_517311251, d_544150384, d_564964481, d_117249500,
d_914594314, d_821247024, d_747006172, d_987563196, d_906417725, d_100767870 , d_878865966, d_255077064
FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP` 
where Connect_ID IS NOT NULL"

parts_table <- bq_project_query(project, parts)
parts_data <- bq_table_download(parts_table, bigint = "integer64")

parts_data$Connect_ID <- as.numeric(parts_data$Connect_ID) ###need to convert type- m1... is double and parts is character

module1= left_join(m1_complete_nodup, parts_data, by="Connect_ID") 
dim(module1)

data_tib_m1 <- as_tibble(module1)
#creating age variable 
data_tib_m1$DOB <- paste0(data_tib_m1$D_544150384, "-", data_tib_m1$D_564964481, "-", data_tib_m1$D_795827569)  #YYYY-MM-DD
data_tib_m1$DOB <- as.Date(data_tib_m1$DOB) #, format='%y-%m-%d') #convert from a string to an actual date
currentDate <- as.Date(Sys.Date(), format='%y/%m/%d') #get todays date
data_tib_m1$AGE <- time_length(difftime(currentDate, data_tib_m1$DOB), "years") #subtract DOB from todays date in years
data_tib_m1$AGE <- round(as.numeric(data_tib_m1$AGE, digits=2)) #make it numeric, you can round as necessary

options(max.print = 10000)  # Increase max.print to a higher number
colnames(data_tib_m1)

#creating binary age (lt40 vs ge40 )
data_tib_m1 <- data_tib_m1 %>%
  mutate(age_cat = case_when(
    AGE < 40 ~ "youngeradult",
    AGE >= 40 & AGE <= 65 ~ "middleadult",   # Less than or equal to 65 years old
    AGE >= 66 & AGE <= 80 ~ "olderadult",  # Greater than 65 years old
    TRUE ~ NA_character_  # Missing age or NA
  ))

module1_activity <- data_tib_m1 %>% 
  select(Connect_ID, AGE, age_cat, d_827220437) %>%
  mutate(Connect_ID = as.character(Connect_ID))

#merging age and site from module 1 with physical activity data
physical_activity_ROI_stratified <-  physical_activity_ROI_BC %>% 
  left_join(module1_activity, by = "Connect_ID")

#examining guideline_cat by age
summary(freqlist(~age_cat, data = physical_activity_ROI_stratified))
age_table <- table(physical_activity_ROI_stratified$guideline_cat, physical_activity_ROI_stratified$age_cat)
age_prop <- prop.table(age_table, margin=1)
print(age_prop)

#examining guideline_cat by site
summary(freqlist(~d_827220437, data = physical_activity_ROI_stratified))
site_table <- table(physical_activity_ROI_stratified$guideline_cat, physical_activity_ROI_stratified$d_827220437)
site_prop <- prop.table(site_table, margin=1)
print(site_prop)

}