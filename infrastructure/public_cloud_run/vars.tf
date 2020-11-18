variable "name" {
  description = "Name for the cloud run service"
  type = string
}

variable "image" {
  description = "path to docker image to run. Should start with gcr.io"
  type = string
}

variable "cpu" {
  description = "CPU resource limit"
  type = string
  default = "1000m"
}

variable "memory" {
  description = "Memory resource limit"
  type = string
  default = "256Mi"
}

variable "ports" {
  description = "ports to expose from container"
  type = list(number)
  default = [8080]
}

variable "env" {
  description = "Environment variables to set on service"
  type = list(object({
    name = string
    value = string
  }))
  default = []
}

variable "gcp_region" {
  type = string
  default = "us-east4"
}

variable "annotations" {
  type = map(string)
  default = {}
}
