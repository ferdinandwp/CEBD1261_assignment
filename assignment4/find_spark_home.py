from __future__ import print_function
import os
import sys


def _find_spark_home():
    """Find the SPARK_HOME."""
    # If the environment has SPARK_HOME set trust it.
    if "SPARK_HOME" in os.environ:
        return os.environ["SPARK_HOME"]

    def is_spark_home(path):
        """Takes a path and returns true if the provided path could be a reasonable SPARK_HOME"""
        return (os.path.isfile(os.path.join(path, "bin/spark-submit")) and
                (os.path.isdir(os.path.join(path, "jars")) or
                 os.path.isdir(os.path.join(path, "assembly"))))

    paths = ["../", os.path.dirname(os.path.realpath(__file__))]

    # Add the path of the PySpark module if it exists
    if sys.version < "3":
        import imp
        try:
            module_home = imp.find_module("pyspark")[1]
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            pass
    else:
        from importlib.util import find_spec
        try:
            module_home = os.path.dirname(find_spec("pyspark").origin)
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            pass

    # Normalize the paths
    paths = [os.path.abspath(p) for p in paths]

    try:
        return next(path for path in paths if is_spark_home(path))
    except StopIteration:
        print("Could not find valid SPARK_HOME while searching {0}".format(paths), file=sys.stderr)
        sys.exit(-1)

if __name__ == "__main__":
    print(_find_spark_home())
