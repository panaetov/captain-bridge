FROM python:3.10-slim-buster

ARG NEXUS_PYPI_INDEX_URL

ENV HOME="/dude"
ENV PYTHONPATH=$HOME

RUN groupadd -g 999 dudes && \
    useradd -u 999 -m -g dudes -s /bin/bash dude && \
    mkdir -p ${HOME} && chown dude:dudes $HOME

WORKDIR ${HOME}

COPY --chown=dude:dudes ./service ${HOME}/service
COPY --chown=dude:dudes ./entrypoints.sh ${HOME}
COPY --chown=dude:dudes ./requirements/requirements.txt ${HOME}
COPY --chown=dude:dudes ./front ${HOME}/front

RUN apt update && apt install -y build-essential && \
    pip install -i "${NEXUS_PYPI_INDEX_URL:-https://pypi.org/simple}" -r "$HOME/requirements.txt"

USER dude
ENTRYPOINT ["/dude/entrypoints.sh"]

CMD ["server"]
