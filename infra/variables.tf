variable "credentials" {
  sensitive   = true
  description = "My Credentials"
}

variable "default_region" {
  type        = string
  description = "Region"
}

variable "default_location" {
  type        = string
  description = "Project Location"
}

variable "project_name" {
  type        = string
  description = "Project"
}

variable "bq_dataset_name" {
  type        = string
  description = "My BigQuery Dataset Name"
}

variable "gcs_bucket_name" {
  type        = string
  description = "My Storage Bucket Name"
}

variable "gcs_storage_class" {
  type        = string
  description = "Bucket Storage Class"
}
