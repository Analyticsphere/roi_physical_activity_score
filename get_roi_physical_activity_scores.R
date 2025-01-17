# Get Physical Activity scores for Return of Information (ROI) reports.
#
# Authors:
# - Brittany Crawford - Analytics
# - Jake Peters - Modified Brittany's code for integration with plumber and Cloud Run.
#
# Description:
# This function connects to a BigQuery database to retrieve and process physical activity data
# for ROI reports.
#
# Parameters:
# - project: A character string specifying the Google Cloud project ID. Defaults to the value
#            of the `PROJECT_ID` environment variable.
# - include_only_updates: A logical flag indicating whether to include only updated records.
#                          If `TRUE`, excludes participants already present in the ROI table.
#                          Defaults to `TRUE`.
#
# Returns:
# A data frame containing the following columns:
# - Connect_ID: Identifier for the participant.
# - guideline_cat: Category of guideline minutes ("not meeting", "meeting", "exceeding").
# - strengthening_binary: Indicates if the participant engages in muscle-strengthening activities (TRUE/FALSE).
# - true_missing: Indicates if the participant skipped all physical activity questions (TRUE/FALSE).
#
# Example Usage:
# # Retrieve physical activity scores using default project ID and excluding existing updates
# roi_scores <- get_roi_physical_activity_scores()
#
# # Retrieve scores for a specific project and include all records regardless of updates
# roi_scores_all <- get_roi_physical_activity_scores(project = "my-special-project", include_only_updates = FALSE)

get_roi_physical_activity_scores <- function(project=Sys.getenv("PROJECT_ID"),
                                             include_only_updates = TRUE) {

  dataset <- "FlatConnect"

  library(dplyr)
  library(DBI)
  library(bigrquery)
  library(glue)

  ## Connect to Database =========================================================
  bigrquery::bq_auth() # Authenticate with BigQuery

  # Establish connection to BigQuery
  con <- DBI::dbConnect(bigrquery::bigquery(), project=project, dataset=dataset, billing=project)

  # Get list of participants that are already in the ROI table
  sql <- glue::glue("SELECT Connect_ID
                     FROM `{project}.ROI.physical_activity`
                     WHERE Connect_ID IS NOT NULL")
  result <- DBI::dbGetQuery(con, sql)
  connect_ids <- as.character(result$Connect_ID)

  if (include_only_updates & length(result$Connect_ID) > 0 ) {
    connect_ids_to_exclude <- paste(shQuote(connect_ids, type = "cmd"), collapse = ", \n")
  } else {
    connect_ids_to_exclude <- "'NONE'"
  }

  # Specify just the data we need with a query
  final_query <- glue::glue(
  "
  WITH m2_dup AS
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
    FROM `{project}.FlatConnect.module2_v2_JP`
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
  FROM `{project}.FlatConnect.module2_v1_JP`
  --Remove participants that completed both v1&v2 from the table for v1
      -- Remove participants that completed both v1 & v2 from the table for v1.
      WHERE Connect_ID NOT IN ('3477605676','8065823194','4505692375','2774891615',
      '6547756854','3715901189','4394283959','8820522355','8731246565','1817817604',
      '9329247892','1996085198','1015390716','8021087753','8134860443','8166039328',
      '8016812218','1105606613','9333929469','8799687034','5671051093','1256197783',
      '2287983457','6367118302','5118827628')
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
      LEFT JOIN `{project}.FlatConnect.module2_v1_JP` AS v1
        ON dup.Connect_ID = v1.Connect_ID
      LEFT JOIN	`{project}.FlatConnect.module2_v2_JP` AS v2
        ON v2.Connect_ID = coalesce(dup.Connect_ID,v1.Connect_ID)
      INNER JOIN `{project}.FlatConnect.participants_JP` AS p
        ON coalesce(dup.Connect_ID, v1.Connect_ID, v2.Connect_ID) = p.Connect_ID
      WHERE
        p.d_821247024 = '197316935'     -- is verified
        AND p.d_747006172 = '104430631' -- has not withdrawn consent
        AND p.d_987563196 = '104430631' -- should not be deceased
        AND p.d_536735468 ='231311385'  -- shuld have submitted module 2
        AND dup.Connect_ID NOT IN ({connect_ids_to_exclude}) -- exclude participants already in table
  ")

  physical_activity_ROI <- DBI::dbGetQuery(con, final_query)

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

  # ----------------------------------------------------------------------------
  #Function to create frequency and duration variables for each activity
    calculate_rec_activity <- function(data, frequency_input, frequency_output, duration_input, duration_output) {
      data <- mutate(data,
                     #Creating frequency variables for each activity
                     !!frequency_output := case_when(
                       !is.na(!!sym(frequency_input)) ~ case_when(  #calculate frequency based on follow up responses
                         !!sym(frequency_input) == 239152340 ~ 0.24,  #one day / 4.25 weeks per month = 0.24 days per week
                         !!sym(frequency_input) == 582006876 ~ 0.59,  #averaged 2.5 days / 4.25 weeks per month = 0.59 days per week
                         !!sym(frequency_input) == 645894551 ~ 1.5,   #averaged 1 to 2 days per week
                         !!sym(frequency_input) == 996315715 ~ 3.5,   #averaged 3 to 4 days per week
                         !!sym(frequency_input) == 671267928 ~ 5.5,   #averaged 5 to 6 days per week
                         !!sym(frequency_input) == 647504893 ~ 7),    #everyday
                       TRUE ~ 0 #default to zero if no follow up response
                     ),
                     #Creating duration variables for each activity
                     !!duration_output := case_when(
                       !is.na(!!sym(duration_input)) ~ case_when(  #calculate duration based on follow up responses
                         !!sym(duration_input) == 428999623 ~ 0.125,  #averaged 7.5 minutes/60 = 0.125 hours
                         !!sym(duration_input) == 248303092 ~ 0.383,  #averaged 23 minutes/60 = 0.383 hours
                         !!sym(duration_input) == 206020811 ~ 0.625,  #averaged 37.5 minutes/60 = 0.625 hours
                         !!sym(duration_input) == 264163865 ~ 0.867,  #averaged 52 minutes/60 = 0.867 hours
                         !!sym(duration_input) == 638092100 ~ 1,      #one hr
                         !!sym(duration_input) == 628177728 ~ 2,	     #two hrs
                         !!sym(duration_input) == 805918496 ~ 3),		 #three hrs
                       TRUE ~ 0 #default to zero if no follow up response
                     ))
      return(data)
    }

    #Calling function that creates freq and dur variable for each activity
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI,"SrvMRE_WalkHikeOften_v1r0","WalkHike_freq","SrvMRE_WalkHikeTime_v1r0","WalkHike_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_JogRunOften_v1r0","JogRun_freq","SrvMRE_JogRunTime_v1r0","JogRun_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_TennisOften_v1r0","Tennis_freq","SrvMRE_TennisTime_v1r0","Tennis_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_GolfOften_v1r0","Golf_freq","SrvMRE_GolfTime_v1r0","Golf_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_SwimLapsOften_v1r0","SwimLaps_freq","SrvMRE_SwimLapsTime_v1r0","SwimLaps_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_BikeOften_v1r0","Bike_freq","SrvMRE_BikeTime_v1r0","Bike_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_StrengthOften_v1r0","Strength_freq","SrvMRE_StrengthTime_v1r0","Strength_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_YogaOften_v1r0","Yoga_freq","SrvMRE_YogaTime_v1r0","Yoga_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_MAOften_v1r0","MA_freq","SrvMRE_MATime_v1r0","MA_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_DanceOften_v1r0","Dance_freq","SrvMRE_DanceTime_v1r0","Dance_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_SkiOften_v1r0","Ski_freq","SrvMRE_SkiTime_v1r0","Ski_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_CCSkiOften_v1r0","CCSki_freq","SrvMRE_CCSkiTime_v1r0","CCSki_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_SurfOften_v1r0","Surf_freq","SrvMRE_SurfTime_v1r0","Surf_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_HICTOften_v1r0","HICT_freq","SrvMRE_HICTTime_v1r0","HICT_dur")
    physical_activity_ROI_BC <- calculate_rec_activity(physical_activity_ROI_BC,"SrvMRE_ExerciseOften_v1r0","Exercise_freq","SrvMRE_ExerciseTime_v1r0","Exercise_dur")

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

  ##Function to create seasonality variables, ensuring all season variables are numeric
  calculate_season <- function(data, spring_input, summer_input, fall_input, winter_input, season_output) {
    data <- mutate(data,
                   !!season_output := case_when(
                     rowSums(cbind(
                       as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))),
                       na.rm = TRUE) == 1 ~ 0.25, # One season selected
                     rowSums(cbind(
                       as.numeric(!!sym(spring_input)),as.numeric(!!sym(summer_input)),as.numeric(!!sym(fall_input)),as.numeric(!!sym(winter_input))),
                       na.rm = TRUE) == 2 ~ 0.50, # Two seasons selected
                     rowSums(cbind(
                       as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)),as.numeric(!!sym(winter_input))),
                       na.rm = TRUE) == 3 ~ 0.75, # Three seasons selected
                     rowSums(cbind(
                       as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))),
                       na.rm = TRUE) == 4 ~ 1.00, # Four seasons selected
                     rowSums(cbind(
                       as.numeric(!!sym(spring_input)), as.numeric(!!sym(summer_input)), as.numeric(!!sym(fall_input)), as.numeric(!!sym(winter_input))),
                       na.rm = TRUE) == 0 ~ 0, # If none selected, set to 0
                   ))
    return(data)
  }

  #Calling function that creates seasonality variables
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_WalkHikeSpring_v1r0", "SrvMRE_WalkHikeSummer_v1r0", "SrvMRE_WalkHikeFall_v1r0", "SrvMRE_WalkHikeWinter_v1r0","WalkHike_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_JogRunSpring_v1r0", "SrvMRE_JogRunSummer_v1r0", "SrvMRE_JogRunFall_v1r0", "SrvMRE_JogRunWinter_v1r0", "JogRun_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_TennisSpring_v1r0", "SrvMRE_TennisSummer_v1r0", "SrvMRE_TennisFall_v1r0", "SrvMRE_TennisWinter_v1r0", "Tennis_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_GolfSpring_v1r0", "SrvMRE_GolfSummer_v1r0", "SrvMRE_GolfFall_v1r0", "SrvMRE_GolfWinter_v1r0", "Golf_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_SwimLapsSpring_v1r0", "SrvMRE_SwimLapsSummer_v1r0", "SrvMRE_SwimLapsFall_v1r0", "SrvMRE_SwimLapsWinter_v1r0", "SwimLaps_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_BikeSpring_v1r0", "SrvMRE_BikeSummer_v1r0", "SrvMRE_BikeFall_v1r0", "SrvMRE_BikeWinter_v1r0", "Bike_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_StrengthSpring_v1r0", "SrvMRE_StrengthSummer_v1r0", "SrvMRE_StrengthFall_v1r0", "SrvMRE_StrengthWinter_v1r0", "Strength_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_YogaSpring_v1r0", "SrvMRE_YogaSummer_v1r0", "SrvMRE_YogaFall_v1r0", "SrvMRE_YogaWinter_v1r0", "Yoga_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_MASpring_v1r0", "SrvMRE_MASummer_v1r0", "SrvMRE_MAFall_v1r0", "SrvMRE_MAWinter_v1r0", "MA_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_DanceSpring_v1r0", "SrvMRE_DanceSummer_v1r0", "SrvMRE_DanceFall_v1r0", "SrvMRE_DanceWinter_v1r0", "Dance_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_SkiSpring_v1r0", "SrvMRE_SkiSummer_v1r0", "SrvMRE_SkiFall_v1r0", "SrvMRE_SkiWinter_v1r0", "Ski_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_CCSkiSpring_v1r0", "SrvMRE_CCSkiSummer_v1r0", "SrvMRE_CCSkiFall_v1r0", "SrvMRE_CCSkiWinter_v1r0", "CCSki_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_SurfSpring_v1r0", "SrvMRE_SurfSummer_v1r0", "SrvMRE_SurfFall_v1r0", "SrvMRE_SurfWinter_v1r0", "Surf_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_HICTSpring_v1r0", "SrvMRE_HICTSummer_v1r0", "SrvMRE_HICTFall_v1r0", "SrvMRE_HICTWinter_v1r0", "HICT_season")
  physical_activity_ROI_BC <- calculate_season(physical_activity_ROI_BC, "SrvMRE_OtherExerciseSpring_v1r0", "SrvMRE_OtherExerciseSummer_v1r0", "SrvMRE_OtherExerciseFall_v1r0", "SrvMRE_OtherExerciseWinter_v1r0", "Exercise_season")

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

  timestamp <- format(
    Sys.time(),
    format = "%Y-%m-%dT%H:%M:%OS3Z",
    tz = "UTC"
  )

  # Prepare final data set so that it conforms with BigQuery schema
  physical_activity_ROI_final <- physical_activity_ROI_BC %>%
    select(Connect_ID, guideline_cat, strengthening_binary, true_missing) %>%
    mutate(
      Connect_ID = as.character(Connect_ID),
      d_449038410 = case_when(
        guideline_cat == "meeting"     ~ "682636404",
        guideline_cat == "not meeting" ~ "104593854",
        guideline_cat == "exceeding"   ~ "948593796"
      ),
      d_205380968 = case_when(
        strengthening_binary == "Yes" ~ "353358909",
        strengthening_binary == "No"  ~ "104430631",
        TRUE ~ NA_character_
      ),
      d_513939926 = case_when(
        true_missing == 1 ~ "353358909", # Yes
        true_missing == 0 ~ "104430631", # No
        TRUE ~ NA_character_
      ),
      "d_416831581" = as.character(timestamp) # Autogenerated Date/Time for ROI PA table updated
      #"d_730304319" = NA_character_  # Flag for Report Delivered
    ) %>%
    filter(d_513939926 == "104430631") %>% # Only include participants for whom "True Missing" is "No"
    select(Connect_ID, d_449038410, d_205380968, d_416831581)

  return(physical_activity_ROI_final)
}
