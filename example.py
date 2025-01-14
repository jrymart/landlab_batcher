from landlab_ensemble.generate_ensembles import create_model_db
from landlab_ensemble.construct_model import ModelDispatcher
from diffusion_streampower_lem import SimpleLem
import pathlib

try:
    pathlib.Path.unlink("diffusion_streampower.db")
except FileNotFoundError:
    print("nothing to clean up")
create_model_db("diffusion_streampower.db", "model_params.json")

dispatcher = ModelDispatcher("diffusion_streampower.db", SimpleLem, "test_output/")
for i in range(50):
    dispatcher.dispatch_model()
