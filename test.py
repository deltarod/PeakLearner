import sys
is_conda = ('anaconda' in sys.executable) or ('miniconda' in sys.executable)

print(is_conda)