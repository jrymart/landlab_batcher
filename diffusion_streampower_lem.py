from landlab.core import load_params
from landlab.components import LinearDiffuser, FlowAccumulator, FastscapeEroder
from model_base import LandlabModel
import numpy as np

class SimpleLem(LandlabModel):

    DEFAULT_PARAMS = {
        "grid": {
            "source": "create",
            "create_grid": {
                "RasterModelGrid": [
                    (41, 5),
                    {"xy_spacing": 5},
                    ],
                },
            },
        "clock": {"start": 0.0, "stop": 1000000, "step": 1250},
        "output": {
            "plot_times": [100, 100000, 1000000],
            "save_times": [1000001],
            "report_times": [1000001],
            "save_path": "model_run",
            "fields": None,
            "plot_to_file":  True,
            },
        "baselevel": {
            "uplift_rate": 0.0001,
            },
        "diffuser": {"D": 0.01},
        "streampower": {"k": 0.01, "m": 0, "n": 2, "threshold": 2}
        }

    def __init__(self, params={}):
        """Initialize the Model"""
        super().__init__(params)

        if not ("topographic__elevation" in self.grid.at_node.keys()):
            self.grid.add_zeros("topographic__elevation", at="node")
        rng = np.random.default_rng(seed=int(params["seed"]))
        grid_noise= rng.random(self.grid.number_of_nodes)/10
        self.grid.at_node["topographic__elevation"] += grid_noise
        self.topo = self.grid.at_node["topographic__elevation"]

        self.uplift_rate = params["baselevel"]["uplift_rate"]
        self.diffuser = LinearDiffuser(
            self.grid,
            linear_diffusivity = params["diffuser"]["D"]
            )
        self.accumulator = FlowAccumulator(self.grid, flow_director="D8")
        self.eroder = FastscapeEroder(self.grid,
                                      K_sp=params["streampower"]["k"],
                                      m_sp=params["streampower"]["m"],
                                      n_sp=params["streampower"]["n"],
                                      threshold_sp=params["streampower"]["threshold"])


    def update(self, dt):
        """Advance the model by one time step of duration dt."""
        if self.current_time % 10000 == 0:
            print("Model %s on year %d" % (self.run_id, self.current_time))
        self.topo[self.grid.core_nodes] += self.uplift_rate * dt
        self.diffuser.run_one_step(dt)
        self.accumulator.run_one_step()
        self.eroder.run_one_step(dt)
        self.current_time += dt

