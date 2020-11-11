variable "gcp_project" {
  type = string
}

variable "gcp_region" {
  type    = string
  default = "us-east4"
}

variable "gcp_zone" {
  type = string
  default = "us-east4-b"
}

variable "api_url" {
  type = string
  default = "https://api.covidcountydata.org"
}

variable "db_instance_name" {
  type    = string
  default = "pg-db"
}

variable "db_version" {
  type    = string
  default = "POSTGRES_12"
}

variable "db_name" {
  type    = string
  default = "covid"
}

variable "downloadable_bucket_name" {
  type    = string
  default = "data_downloadables"
}

variable "nha_data_bucket_name" {
  type    = string
  default = "can-ccd-private-data"
}

variable "gcf_source_bucket_name" {
  type    = string
  default = "gcf_sources"
}

variable "airflow_db_name" {
  type    = string
  default = "airflow"
}

variable "db_user_name" {
  type    = string
  default = "pguser"
}

variable "mixpanel_token" {
  type    = string
  default = ""
}

variable "home_ip" {
  type    = string
  default = null
}
