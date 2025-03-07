#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# DO NOT MODIFY
def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    
    run = wandb.init(project="nyc_airbnb", group="cleaning", save_code=True)
    artifact_local_path = run.use_artifact("lbekel-western-governors-university/nyc_airbnb/sample.csv:latest").file()
    df = pd.read_csv(artifact_local_path)
    logger.info("Removing out-of-area listings outside NYC boundaries")

    # NYC longitude and latitude boundaries
    nyc_bounds = {
        "min_lat": 40.4774,
        "max_lat": 40.9176,
        "min_lon": -74.2591,
        "max_lon": -73.7004
    }

    # Keep only listings within NYC
    df = df[
        (df["latitude"] >= nyc_bounds["min_lat"]) & (df["latitude"] <= nyc_bounds["max_lat"]) &
        (df["longitude"] >= nyc_bounds["min_lon"]) & (df["longitude"] <= nyc_bounds["max_lon"])
    ]

    logger.info(f"Remaining data points after NYC boundary filtering: {df.shape[0]}")

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    # idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    # df = df[idx].copy()
    # Save the cleaned file
    df.to_csv('clean_sample.csv',index=False)

    # log the new data.
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
 )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


# TODO: In the code below, fill in the data type for each argumemt. The data type should be str, float or int. 
# TODO: In the code below, fill in a description for each argument. The description should be a string.
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")
  
    parser.add_argument(
        "--input_artifact", 
        type = str,## INSERT TYPE HERE: str, float or int,
        help = "The name of the input artifact (raw dataset) stored in Weights & Biases",## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--output_artifact", 
        type = str, ## INSERT TYPE HERE: str, float or int,
        help = "The name for the cleaned dataset artifact to be stored in Weights & Biases", ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--output_type", 
        type = str, ## INSERT TYPE HERE: str, float or int,
        help = "clean_sample", ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--output_description", 
        type = str, ## INSERT TYPE HERE: str, float or int,
        help = "Cleaned Airbnb dataset", ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--min_price", 
        type = float, ## INSERT TYPE HERE: str, float or int,
        help = "Minimum price for listings to be considered valid", ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--max_price",
        type = float,## INSERT TYPE HERE: str, float or int,
        help = "Maximum price for listings to be considered valid",## INSERT DESCRIPTION HERE,
        required = True
    )


    args = parser.parse_args()

    go(args)