from pipelines.airport_pipeline import run
import logging


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# setup(name='Your program',
#     version='0.7.0',
#     description='Your desccription',
#     packages=['foo', 'foo.bar'], # add `foo.bar` here
