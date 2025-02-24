terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_name
  region      = var.default_region
}

resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.default_location
  force_destroy = true
  storage_class = var.gcs_storage_class


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.default_location
}

resource "google_bigquery_table" "green_tripdata" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "green_tripdata"

  deletion_protection = false
}

resource "google_bigquery_table" "yellow_tripdata" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "yellow_tripdata"

  deletion_protection = false
}

resource "google_bigquery_table" "fhv_tripdata" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "fhv_tripdata"

  deletion_protection = false
}

# resource "google_bigquery_job" "create_green_tripdata" {
#   job_id   = "create-green-tripdata"
#   project  = var.project_name
#   location = var.default_location

#   query {
#     query = <<EOF
#     CREATE OR REPLACE TABLE `${var.project_name}.${google_bigquery_dataset.dataset.dataset_id}.green_tripdata` AS
#     SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`;

#     INSERT INTO `${var.project_name}.${google_bigquery_dataset.dataset.dataset_id}.green_tripdata`
#     SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;
#     EOF

#     use_legacy_sql = false
#   }

#   depends_on = [google_bigquery_table.green_tripdata]
# }



# resource "google_bigquery_job" "create_yellow_tripdata" {
#   job_id   = "create-yellow-tripdata"
#   project  = var.project_name
#   location = var.default_location

#   query {
#     query = <<EOF
#     CREATE OR REPLACE TABLE `${var.project_name}.${google_bigquery_dataset.dataset.dataset_id}.yellow_tripdata` AS
#     SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;

#     INSERT INTO `${var.project_name}.${google_bigquery_dataset.dataset.dataset_id}.yellow_tripdata`
#     SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;
#     EOF

#     use_legacy_sql = false
#   }

#   depends_on = [google_bigquery_table.yellow_tripdata]
# }
