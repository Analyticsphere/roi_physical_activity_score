library(plumber)
library(dplyr)
library(DBI)
library(bigrquery)
library(glue)

#* @apiTitle Return of Information
#* @apiDescription Plumber example description.

#* Heartbeat endpoint for testing
#* @get /heartbeat
#* @response 200 A message indicating the API is alive
function() {
  list(
    status = "success",
    message = "API is running",
    timestamp = Sys.time()
  )
}

#* Generate physical activity scores and write to BigQuery table.
#* @param project_id GCP project
#* @post /update_roi_physical_activity_data
function(project_id="nih-nci-dceg-connect-stg-5519") {

  # Define dataset and table information
  dataset_id <- "ReturnOfInformation"
  table_id   <- "physical_activity"

  # Get data
  source('get_roi_physical_activity_scores.R')
  data_to_append <- get_roi_physical_activity_scores(project_id,
                                                     include_only_updates=TRUE)

  # Check how many rows are in the data to append
  num_rows <- nrow(data_to_append)

  # Convert the data frame to a BigQuery table object
  bq_table <- bigrquery::bq_table(project=project_id,
                                  dataset=dataset_id,
                                  table=table_id)

  # Append the data to the existing table
  bq_table_upload(
    x = bq_table,
    values = data_to_append,
    fields = NULL, # BigQuery infers schema from the existing table
    create_disposition = "CREATE_NEVER",  # Do not create a new table
    write_disposition  = "WRITE_APPEND"
  )

  # Print the number of rows appended
  message <- glue(
    "Successfully appended {num_rows} rows to {dataset_id}.{table_id}.\n"
    )
  cat(message)
  return(message)

}

