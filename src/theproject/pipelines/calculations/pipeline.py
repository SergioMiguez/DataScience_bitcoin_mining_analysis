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
from theproject.pipelines.calculations.nodes import (
    modifyStringto,
    modifyStringtofloat,
    calculate_profit,
    modify_process_energy,
    calculate_substraction,
    graph,
    co2_emissions,
    graph2,
)


def create_pipeline(**kwargs):
    
    return Pipeline(
        [
            node(
                modifyStringtofloat,
                inputs = "formated_total_bitcoins",
                outputs = "new_total_bitcoins",
                name= "transforming_to_float",
            ),
            node(
                modifyStringto,
                inputs="reversed_cryptocurrencies",
                outputs= "new_cryptocurrencies",
                name= "modify_to_int",
            ),
            node(
                calculate_profit,
                inputs=["new_cryptocurrencies","new_total_bitcoins"],
                outputs= "profit",
                name= "profit_calculation",
            ),
            node(
                modify_process_energy,
                inputs="processed_energy",
                outputs= "new_process_energy",
                name= "process_energy_calculation",
            ),
            node(
                calculate_substraction,
                inputs=["profit","new_process_energy", "reversed_cryptocurrencies"],
                outputs= "benefit",
                name= "process_benefit",
            ),
            node(
                graph,
                inputs="benefit",
                outputs= None,
                name= "plot_graph",
            ),
             node(
                co2_emissions,
                inputs=["new_process_energy","reversed_cryptocurrencies"],
                outputs= "co2_emission",
                name= "emissions",
             ),
             node(
                graph2,
                inputs="co2_emission",
                outputs= None,
                name= "plot_graph2",
            ),
                       


        ], tags = ["cp_tag"],
    )
