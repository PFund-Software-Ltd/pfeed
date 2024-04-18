#!/bin/bash

echo "Google Analytics ID: $GA_TRACKING_ID"

echo "Before replacement:"
grep "google_analytics_id" _config.yml

# Replace the placeholder in the YAML file with the actual Google Analytics ID
sed -i "s/{GA_TRACKING_ID_PLACEHOLDER}/$GA_TRACKING_ID/" _config.yml

echo "After replacement:"
grep "google_analytics_id" _config.yml

# Build
jupyter-book build docs/ --all