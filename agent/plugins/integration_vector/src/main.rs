mod vector_app;
pub mod vector_component;
mod vector_config;

use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

use serde;
use serde_yaml;

const ERROR_CONFIG: &str = "
global:
  inputs:
    vector:
      data_dir: /home/taloric/vector
      sources:
        demo_logs:
          type: demo_logs
          format: json
          lines:
          - line1
      sinks:
        console:
          type: console
          inputs:
          - test_error
          encoding:
            codec: json
";

const TEST_YAML_CONFIG: &str = "
global:
  inputs:
    vector:
      data_dir: /home/taloric/vector
      sources:
        demo_logs:
          type: demo_logs
          format: json
          interval: 3
          lines:
          - line1
      sinks:
        console:
          type: console
          inputs:
          - demo_logs
          encoding:
            codec: json
";

const TEST_YAML_CONFIG_2: &str = "
global:
  inputs:
    vector:
      data_dir: /home/taloric/vector
      sources:
        demo_logs:
          type: demo_logs
          format: json
          interval: 2
          lines:
          - line1
      sinks:
        console:
          type: console
          inputs:
          - demo_logs
          encoding:
            codec: json
";

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct Config {
    global: Global,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct Global {
    inputs: Inputs,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct Inputs {
    // vector: vector::config::ConfigBuilder,
    vector: serde_yaml::Value,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    current_time();
    let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let result = serde_yaml::from_str::<Config>(TEST_YAML_CONFIG)?;

    let mut comp = vector_component::VectorComponent::new(
        true,
        result.global.inputs.vector.clone(),
        runtime.clone(),
    );
    comp.start();

    thread::sleep(std::time::Duration::from_secs(10));
    current_time();
    if let Some(handle) = comp.notify_stop() {
        handle.join().unwrap();
    }
    // -----
    thread::sleep(std::time::Duration::from_secs(10));
    current_time();

    let result = serde_yaml::from_str::<Config>(TEST_YAML_CONFIG_2)?;
    comp.on_config_change(true, result.global.inputs.vector.clone());

    thread::sleep(std::time::Duration::from_secs(15));
    current_time();

    if let Some(handle) = comp.notify_stop() {
        handle.join().unwrap();
    }
    thread::sleep(std::time::Duration::from_secs(10));
    current_time();

    Ok(())
}

fn current_time() {
    println!(
        "current_time {}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
}
