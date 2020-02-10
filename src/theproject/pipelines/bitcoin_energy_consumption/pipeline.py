
from kedro.pipeline import node, Pipeline
from theproject.pipelines.bitcoin_energy_consumption.nodes import (
    process_bitcoin_energy_consumption,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                process_bitcoin_energy_consumption,
                inputs="annual_energy_bitcoin",
                outputs="processed_energy_bitcoin_consumption",
                name="processing_energy_bitcoin_consumption",
            ),

        ],  tags = ["becd_tag"], 
    )
