terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.113"
    }
    random = {
      source = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "location" { default = "eastus" }
variable "resource_group_name" { default = "rg-message-transfer-poc" }
variable "blue_namespace" { default = "shan-bg-blue" }
variable "green_namespace" { default = "shan-bg-green" }
variable "blue_queue" { default = "orders-scheduled-blue" }
variable "green_queue" { default = "orders-scheduled-green" }
variable "redis_name" { default = "mtpoc-redis" }
variable "sku_name" { default = "Basic" }
variable "sku_capacity" { default = 0 }

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_servicebus_namespace" "blue" {
  name                = var.blue_namespace
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Basic"
}

resource "azurerm_servicebus_namespace" "green" {
  name                = var.green_namespace
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Basic"
}

resource "azurerm_servicebus_queue" "blueq" {
  name                = var.blue_queue
  resource_group_name = azurerm_resource_group.rg.name
  namespace_name      = azurerm_servicebus_namespace.blue.name
  enable_partitioning = false
}

resource "azurerm_servicebus_queue" "greenq" {
  name                = var.green_queue
  resource_group_name = azurerm_resource_group.rg.name
  namespace_name      = azurerm_servicebus_namespace.green.name
  enable_partitioning = false
}

# (Optional) Small Azure Cache for Redis; for POC you can just use local Docker Redis.
resource "azurerm_redis_cache" "redis" {
  name                = var.redis_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  capacity            = 0
  family              = "C"
  sku_name            = "Basic"
  minimum_tls_version = "1.2"
}

output "blue_primary_connection_string" {
  value     = azurerm_servicebus_namespace.blue.default_primary_connection_string
  sensitive = true
}
output "green_primary_connection_string" {
  value     = azurerm_servicebus_namespace.green.default_primary_connection_string
  sensitive = true
}
output "redis_hostname" {
  value = azurerm_redis_cache.redis.hostname
}
output "redis_primary_key" {
  value     = azurerm_redis_cache.redis.primary_access_key
  sensitive = true
}
