resource "google_cloud_run_service" "cr_service" {
  name = var.name
  location = var.gcp_region

  template {
    metadata {
      annotations = merge({"autoscaling.knative.dev/maxScale" = "1000"}, var.annotations)
    }
    spec {
      containers {
        image = var.image

        dynamic "env" {
          for_each = var.env

          content {
            name = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            "cpu" = var.cpu
            "memory" = var.memory
          }
        }

        dynamic "ports" {

          for_each = var.ports
          content {
            container_port = ports.value
          }
        }
      }
    }
  }
}


resource "google_cloud_run_service_iam_policy" "noauth" {
  location    = google_cloud_run_service.cr_service.location
  project     = google_cloud_run_service.cr_service.project
  service     = google_cloud_run_service.cr_service.name
  policy_data = data.google_iam_policy.noauth.policy_data
}
