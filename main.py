import probe
from autoupdater import updater
__version__ = "0.0.1"

if __name__ == "__main__":
    updater.check_for_update()
    probe.run()