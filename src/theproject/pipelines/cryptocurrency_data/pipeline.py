# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
from kedro.pipeline import node, Pipeline
from theproject.pipelines.cryptocurrency_data.nodes import (
    preprocess_cryptocurrencies,
    filter_cryptocurrencies,
    format_cryptocurrencies,
    yearvalid_cryptocurrencies,
    inverse_cryptocurrencies,
    format_totalbitcoins,
    
)


def create_pipeline(**kwargs):
    
    return Pipeline(
        [
            node(
                preprocess_cryptocurrencies,
                inputs="cryptocurrency",
                outputs="processed_cryptocurrencies",
                name="preprocessing_currencies",
            ),
            node(
                filter_cryptocurrencies,
                inputs="processed_cryptocurrencies",
                outputs="filtered_cryptocurrencies",
                name="filtering_cryptocurrencies",
            ),
            node(
                format_cryptocurrencies,
                inputs="filtered_cryptocurrencies",
                outputs="formated_cyptocurrencies",
                name="formating_cryptocurrencies",
            ),
            node(
                yearvalid_cryptocurrencies,
                inputs="formated_cyptocurrencies",
                outputs="validyear_cryptocurrencies",
                name="validatingyear_cryptocurrencies",
            ),
            node(
                inverse_cryptocurrencies,
                inputs="validyear_cryptocurrencies",
                outputs="reversed_cryptocurrencies",
                name="reversing_cryptocurrencies",
            ),
            node(
                format_totalbitcoins,
                inputs="total-bitcoins",
                outputs="formated_total_bitcoins",
                name="formating_total_bitcoins",
            ),
             
        
            


        ], tags = ["cd_tag"],
    )
