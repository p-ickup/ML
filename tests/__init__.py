"""Test package.

Pipeline modules live in ``Algorithm/`` and use absolute imports
(``import config``). Put that directory on ``sys.path`` when the test
package is imported so test modules can import the pipeline directly.
"""

import os
import sys

_ALGORITHM_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Algorithm")
if _ALGORITHM_DIR not in sys.path:
    sys.path.insert(0, _ALGORITHM_DIR)
