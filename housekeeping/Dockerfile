# proj1102 - housekeeping health-probe container.
#
# The pipeline is serverless, so only this admin probe is containerised: a
# stateless command-line health check that runs the same on a laptop or a CI
# runner, with a fixed Python + boto3 + pandas + pyarrow stack.
#
# Build:
#   docker build -f housekeeping/Dockerfile -t proj1102-housekeeping .
#
# Run (mounts AWS credentials read-only into the probe user's home):
#   docker run --rm \
#     -v ~/.aws:/home/probe/.aws:ro \
#     -e PROFILE=ralph -e REGION=eu-west-1 \
#     proj1102-housekeeping

FROM python:3.12-slim

LABEL org.opencontainers.image.title="proj1102-housekeeping"
LABEL org.opencontainers.image.description="One-shot pipeline health probe."
LABEL org.opencontainers.image.source="https://github.com/Francesco-Barillari/ccbda-project"

WORKDIR /app

# Pin the same major versions as the Lambda layer so behaviour matches prod.
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir \
        "boto3>=1.34,<2" \
        "pandas>=2.0,<3" \
        "pyarrow>=15.0,<20"

COPY housekeeping/check_pipeline.py /app/check_pipeline.py

# Non-root user for minimal hardening.
RUN useradd --create-home --shell /bin/bash probe \
 && chown -R probe /app
USER probe

ENTRYPOINT ["python", "/app/check_pipeline.py"]
