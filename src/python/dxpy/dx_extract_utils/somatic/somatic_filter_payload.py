import json
from ..exceptions import err_exit, ResourceNotFound
import dxpy

extract_utils_basepath = os.path.join(
    os.path.dirname(dxpy.__file__), "dx_extract_utils/somatic"
)