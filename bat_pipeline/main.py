import logging

from pipelines.airport_pipeline import run

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

