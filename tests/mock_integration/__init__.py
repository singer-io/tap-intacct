import sys
from unittest.mock import MagicMock


for module_name in [
	"boto3",
	"botocore",
	"botocore.credentials",
	"botocore.exceptions",
	"botocore.session",
	"singer_encodings",
	"singer_encodings.csv",
]:
	if module_name not in sys.modules:
		sys.modules[module_name] = MagicMock()
