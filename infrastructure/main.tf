
provider "google" {
  project     = var.gcp_project
  region      = var.gcp_region
  zone = var.gcp_zone
}

resource "google_cloud_run_service" "apiclient" {
  name     = "apiclient"
  location = var.gcp_region

  template {
    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/apiclient:latest"
        ports {
          container_port = 8080
        }
        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256Mi"
          }
        }
      }
    }
  }
}

output "cloudrun_apiclient_url" {
  value = google_cloud_run_service.apiclient.status[0].url
}

resource "google_cloud_run_service" "clean_swagger" {
  name     = "clean-swagger"
  location = var.gcp_region

  template {
    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/clean_swagger:latest"
        ports {
          container_port = 8080
        }
        env {
          name = "API_URL"
          value = "https://api.covidcountydata.org"
        }
        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256M"
          }
        }
      }
    }
  }
}

output "cloudrun_clean_swagger_url" {
  value = google_cloud_run_service.clean_swagger.status[0].url
}

resource "google_cloud_run_service" "postgraphile" {
  name     = "postgraphile"
  location = var.gcp_region

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
      }
    }

    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/postgraphile:latest"

        env {
          name  = "POSTGRAPHILE_DATABASE_URL"
          value = "socket://postgrest:iyeFVFxdFxdTtsJlMmY0@/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}?db=${var.db_name}"
        }
        env {
          name  = "ADMIN_DATABASE_URL"
          value = "socket://${var.db_user_name}:${random_password.db_user_password.result}@/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}?db=${var.db_name}"
        }
        env {
          name  = "POSTGRAPHILE_DB_SCHEMA"
          value = "api"
        }

        ports {
          container_port = 8080
        }

        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "1Gi"
          }
          requests = {}
        }
      }
    }
  }
}

output "cloudrun_postgraphile_url" {
  value = google_cloud_run_service.postgraphile.status[0].url
}

resource "google_cloud_run_service" "postgrest" {
  name     = "postgrest"
  location = var.gcp_region

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
      }
    }

    spec {

      containers {
        image = "gcr.io/${var.gcp_project}/postgrest:latest"
        ports {
          container_port = 3000
        }
        env {
          name = "PGRST_SERVER_PROXY_URL"
          value = var.api_url
        }
        env {
          name = "PGRST_DB_SCHEMA"
          value = "api, meta"
        }
        env {
          name = "PGRST_DB_ANON_ROLE"
          value = "covid_anon"
        }
        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "512Mi"
          }
        }
      }
    }
  }
}

output "cloudrun_postgrest_url" {
  value = google_cloud_run_service.postgrest.status[0].url
}

# resource "google_cloud_run_service" "audits" {
#   name     = "audits"
#   location = var.gcp_region


#   template {
#     metadata {
#       annotations = {
#         "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
#       }
#     }

#     spec {
#       containers {
#         image = "gcr.io/${var.gcp_project}/audits:latest"

#         env {
#           name  = "SQL_CONN_STR"
#           value = "postgres+pg8000://${var.db_user_name}:${random_password.db_user_password.result}@/${var.db_name}?unix_sock=/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}/.s.PGSQL.5432"
#         }

#         ports {
#           container_port = 8080
#         }

#         resources {
#           limits = {
#             "cpu"    = "1000m"
#             "memory" = "256Mi"
#           }
#         }
#       }
#     }
#   }
# }

resource "google_cloud_run_service" "can_nha_reports" {
  name     = "can-nha-reports"
  location = var.gcp_region

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
      }
    }

    spec {

      containers {
        image = "gcr.io/${var.gcp_project}/can-nha-reports:latest"

        env {
          name  = "SQL_CONN_STR"
          value = "postgres://${var.db_user_name}:${random_password.db_user_password.result}@/${var.db_name}?host=/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
        }

        ports {
          container_port = 8080
        }

        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "2Gi"
          }
        }
      }
    }
  }
}

output "cloudrun_can_nha_reports_url" {
  value = google_cloud_run_service.can_nha_reports.status[0].url
}

resource "google_cloud_run_service" "latest_download" {
  name     = "latest-download"
  location = var.gcp_region

  template {
    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/latest_download:latest"

        env {
          name = "GCP_BUCKET"
          value = google_storage_bucket.downloadables_bucket.name
        }

        ports {
          container_port = 8080
        }
        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256Mi"
          }
        }
      }
    }
  }
}

output "cloudrun_latest_download_url" {
  value = google_cloud_run_service.latest_download.status[0].url
}

resource "google_cloud_run_service" "metrics" {
  name     = "metrics"
  location = var.gcp_region

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
      }
    }

    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/metrics:latest"

        env {
          name  = "SQL_CONN_STR"
          value = "postgres+pg8000://${var.db_user_name}:${random_password.db_user_password.result}@/${var.db_name}?unix_sock=/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}/.s.PGSQL.5432"
        }
        env {
          name  = "MIXPANEL_TOKEN"
          value = var.mixpanel_token
        }

        ports {
          container_port = 8080
        }

        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256M"
          }
        }
      }
    }
  }
}

output "cloudrun_metrics_url" {
  value = google_cloud_run_service.metrics.status[0].url
}

resource "google_cloud_run_service" "reports" {
  name     = "reports"
  location = var.gcp_region

  template {
    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/reports:latest"

        ports {
          container_port = 8080
        }

        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256Mi"
          }
        }
      }
    }
  }
}

output "cloudrun_reports_url" {
  value = google_cloud_run_service.reports.status[0].url
}

resource "google_cloud_run_service" "variable_names" {
  name     = "variable-names"
  location = var.gcp_region

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = "${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}"
      }
    }

    spec {
      containers {
        image = "gcr.io/${var.gcp_project}/variable-names:latest"

        env {
          name  = "SQL_CONN_STR"
          value = "postgres+pg8000://${var.db_user_name}:${random_password.db_user_password.result}@/${var.db_name}?unix_sock=/cloudsql/${var.gcp_project}:${var.gcp_region}:${google_sql_database_instance.db_instance.name}/.s.PGSQL.5432"
        }
        env {
          name = "CLEAN_SWAGGER_URL"
          value = google_cloud_run_service.clean_swagger.status[0].url
        }

        ports {
          container_port = 8080
        }

        resources {
          limits = {
            "cpu"    = "1000m"
            "memory" = "256Mi"
          }
        }
      }
    }
  }
}

output "cloudrun_variable_names_url" {
  value = google_cloud_run_service.variable_names.status[0].url
}

resource "google_sql_database_instance" "db_instance" {
  name                = var.db_instance_name
  region              = var.gcp_region
  database_version    = var.db_version
  deletion_protection = false
  settings {
    # tier = "db-custom-8-24576"
    tier = "db-custom-4-12288"
    ip_configuration {
      authorized_networks {
        name  = "spencer-home"
        value = var.home_ip
      }
      authorized_networks {
        name = "open"
        value = "0.0.0.0/0"
      }
    }
  }
}

output "cloud_sql_ip" {
  value = google_sql_database_instance.db_instance.ip_address.0.ip_address
}

output "cloud_sql_sqlalchemy" {
  value = "postgresql://${var.db_user_name}:${random_password.db_user_password.result}@${google_sql_database_instance.db_instance.ip_address.0.ip_address}/${var.db_name}"
}

resource "google_sql_database" "db" {
  name     = var.db_name
  instance = google_sql_database_instance.db_instance.name
}


resource "random_password" "db_user_password" {
  length           = 16
  special          = true
  override_special = "_%@"
}

resource "google_sql_user" "db_user" {
  name     = var.db_user_name
  instance = google_sql_database_instance.db_instance.name
  password = random_password.db_user_password.result
}

resource "google_storage_bucket" "downloadables_bucket" {
  name = "${var.gcp_project}-${var.downloadable_bucket_name}"
}

resource "google_storage_bucket" "nha_bucket" {
  name = "${var.gcp_project}-${var.nha_data_bucket_name}"
}

# google cloud func for NHA storage bucket
resource "google_storage_bucket" "functions_bucket" {
  name = "${var.gcp_project}-${var.gcf_source_bucket_name}"
}

data "archive_file" "nha_data_ingest_trigger" {
  type        = "zip"
  source_dir  = "${path.root}/../services/nha_data_ingest_trigger"
  output_path = "${path.root}/../services/nha_data_ingest_trigger.zip"
}

# place the zip-ed code in the bucket
resource "google_storage_bucket_object" "nha_data_ingest_trigger_func_zip" {
  name   = "services/nha_data_ingest_trigger.zip"
  bucket = google_storage_bucket.functions_bucket.name
  source = "${path.root}/../services/nha_data_ingest_trigger.zip"
}

resource "google_cloudfunctions_function" "nha_data_ingest_trigger_storage_func" {
  name                  = "nha_data_ingest_trigger"
  description           = "Trigger nha cloud run service when object finalized on nha bucket"
  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.functions_bucket.name
  source_archive_object = google_storage_bucket_object.nha_data_ingest_trigger_func_zip.name
  timeout               = 60
  entry_point           = "nha_data_ingest_trigger"
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.nha_bucket.name
  }
  environment_variables = {
    NHA_SERVICE_URL = google_cloud_run_service.can_nha_reports.status[0].url
  }
  runtime = "python37"
}

resource "google_container_registry" "registry" {
  project  = var.gcp_project
  location = "US"
}

# kong vm
resource "google_compute_address" "kong_ip" {
  name="kong-ip"
}

resource "google_compute_instance" "kong_vm" {
  name = "kong"
  machine_type="e2-medium"
  zone = "${var.gcp_region}-b"
  tags = ["sglyon", "kong", "http-server", "https-server"]
  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.kong_ip.address
    }
  }
}

output "kong_ip" {
 value = google_compute_address.kong_ip.address
}

resource "google_compute_firewall" "sglyon_home" {
  name = "sglyon-home"
  network = "default"
  allow {
    protocol = "all"
  }
  source_ranges = [var.home_ip]
  target_tags = ["sglyon"]
}

resource "google_compute_firewall" "kong_data" {
  name = "kong-data"
  network = "default"
  allow {
    protocol = "tcp"
    ports = ["8000"]
  }
  source_ranges = ["0.0.0.0/0"]
  target_tags = ["kong"]
}
