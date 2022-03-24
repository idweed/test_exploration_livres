FROM scrapinghub/scrapinghub-stack-scrapy:2.1
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y zip unzip procps

#============================================
# Google Chrome
#============================================
# can specify versions by CHROME_VERSION;
#  e.g. google-chrome-stable=53.0.2785.101-1
#       google-chrome-beta=53.0.2785.92-1
#       google-chrome-unstable=54.0.2840.14-1
#       latest (equivalent to google-chrome-stable)
#       google-chrome-beta  (pull latest beta)
#============================================

ARG CHROME_VERSION="google-chrome-stable"


RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update -qqy \
  && apt-get -qqy install \
    ${CHROME_VERSION:-google-chrome-stable} \
  && rm /etc/apt/sources.list.d/google-chrome.list \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

#RUN wget -O /opt/crawlera-headless-proxy https://github.com/scrapinghub/crawlera-headless-proxy/releases/download/1.2.1/crawlera-headless-proxy-linux-amd64 \
#    && chmod 777 /opt/crawlera-headless-proxy \
#    && sudo ln -fs /opt/crawlera-headless-proxy /usr/bin/crawlera-headless-proxy
#============================================
# crawlera-headless-proxy
#============================================

RUN curl -L https://github.com/scrapinghub/crawlera-headless-proxy/releases/download/1.2.1/crawlera-headless-proxy-linux-amd64 -o /usr/local/bin/crawlera-headless-proxy \
 && chmod +x /usr/local/bin/crawlera-headless-proxy
#============================================
# Chrome Webdriver
#============================================
# can specify versions by CHROME_DRIVER_VERSION
# Latest released version will be used by default
#============================================
ARG CHROME_DRIVER_VERSION
RUN CHROME_STRING=$(google-chrome --version) \
  && CHROME_VERSION_STRING=$(echo "${CHROME_STRING}" | grep -oP "\d+\.\d+\.\d+\.\d+") \
  && CHROME_MAYOR_VERSION=$(echo "${CHROME_VERSION_STRING%%.*}") \
  && wget --no-verbose -O /tmp/LATEST_RELEASE "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAYOR_VERSION}" \
  && CD_VERSION=$(cat "/tmp/LATEST_RELEASE") \
  && rm /tmp/LATEST_RELEASE \
  && if [ -z "$CHROME_DRIVER_VERSION" ]; \
     then CHROME_DRIVER_VERSION="${CD_VERSION}"; \
     fi \
  && CD_VERSION=$(echo $CHROME_DRIVER_VERSION) \
  && echo "Using chromedriver version: "$CD_VERSION \
  && wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/$CD_VERSION/chromedriver_linux64.zip \
  && rm -rf /opt/selenium/chromedriver \
  && unzip /tmp/chromedriver_linux64.zip -d /opt/selenium \
  && rm /tmp/chromedriver_linux64.zip \
  && mv /opt/selenium/chromedriver /opt/selenium/chromedriver-$CD_VERSION \
  && chmod 755 /opt/selenium/chromedriver-$CD_VERSION \
  && sudo ln -fs /opt/selenium/chromedriver-$CD_VERSION /usr/bin/chromedriver

#============================================
# Firefox and geckodriver
#============================================
#RUN apt-get update                             \
# && apt-get install -y --no-install-recommends \
#    ca-certificates curl firefox-esr           \
# && rm -fr /var/lib/apt/lists/*                \
# && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.24.0/geckodriver-v0.24.0-linux64.tar.gz | tar xz -C /usr/local/bin
RUN apt-get update                             \
 && apt-get install -y --no-install-recommends \
#    ca-certificates curl firefox           \
	ca-certificates curl \
	bzip2 \
	libdbus-glib-1-2 \
&& rm -fr /var/lib/apt/lists/*
RUN wget -q https://ftp.mozilla.org/pub/firefox/releases/60.9.0esr/linux-x86_64/en-US/firefox-60.9.0esr.tar.bz2 \
&& tar xvf firefox-60.9.0esr.tar.bz2 \
&& mv firefox/ /usr/lib/firefox \
&& ln -s /usr/lib/firefox/firefox /usr/bin/firefox \
&& apt-get install -y --no-install-recommends libxt6 libx11-xcb1 libgtk-3-0 \
&& curl -L https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz | tar xz -C /usr/local/bin \
&& chmod +x /usr/local/bin/geckodriver

RUN sudo apt-get install -y lsof

#COPY ./start-crawl /usr/local/bin/start-crawl
#RUN chmod +x /usr/local/bin/start-crawl

ENV PATH="/usr/bin/:/usr/bin:/usr/local/bin:${PATH}"
ENV TERM xterm
ENV SCRAPY_SETTINGS_MODULE albert.settings
ENV CRAWLERA_HEADLESS_APIKEY ca8e16eeca8e4c9da8dc39383670b2da
RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN python setup.py install
#ENTRYPOINT ['crawlera-headless-proxy', '-p', '3128', '-a', '"ca8e16eeca8e4c9da8dc39383670b2da"', '-x', 'profile=desktop']



