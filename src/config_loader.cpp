//
// Created by lexyn on 25-7-14.
//
// src/config_loader.cpp
#include "config_loader.h"
#include "spdlog/spdlog.h"

bool ConfigLoader::load(const std::string& config_path) {
    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(config_path.c_str()) != tinyxml2::XML_SUCCESS) {
        spdlog::critical("Failed to load config file: {}", config_path);
        return false;
    }

    // 解析<Universe>节点（文档1-6）
    auto* universe_node = doc.FirstChildElement("Tsaigu")->FirstChildElement("Universe");
    if (!universe_node) {
        spdlog::critical("Missing <Universe> in config");
        return false;
    }
    universe.calculate_date = universe_node->Attribute("calculate_date");
    universe.stock_universe = universe_node->Attribute("stock_universe");
    universe_node->QueryIntAttribute("pre_days", &universe.pre_days);

    // 解析<Modules>下的<Module>（文档1-6）
    auto* modules_node = doc.FirstChildElement("Tsaigu")->FirstChildElement("Modules");
    for (auto* module_node = modules_node->FirstChildElement("Module");
         module_node; module_node = module_node->NextSiblingElement("Module")) {
        ModuleConfig module;
        module.handler = module_node->Attribute("handler");
        module.name = module_node->Attribute("name");
        module.id = module_node->Attribute("id");
        module.path = module_node->Attribute("path");
        module.frequency = module_node->Attribute("frequency");
        modules.push_back(module);
    }

    spdlog::info("Config loaded successfully");
    return true;
}