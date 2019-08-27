require(['d3js'], (d3) => {
  function getQueryElements(container, query) {
    const queryContainer = container.getElementsByClassName(query)[0];

    return {
      container: queryContainer,
      chart: queryContainer.getElementsByClassName('chart')[0],
      svg: d3.select('#' + container.id + ' .' + query + ' svg'),
      progressBar: queryContainer.getElementsByClassName('progress')[0],
      progressLabel: queryContainer.getElementsByClassName('progress-label')[0]
    }
  }

  function dayOfWeekToString(dayOfWeek) {
    const daysOfWeek = {
      1: 'Mon',
      2: 'Tue',
      3: 'Wed',
      4: 'Thu',
      5: 'Fri',
      6: 'Sat',
      7: 'Sun'
    };

    return daysOfWeek[dayOfWeek]
  }

  const errorBarColor = 'black';
  const sockets = {};

  const start = (progressive, queryId, view) => {
    const container = document.getElementById(progressive ? 'progressive' : 'native');

    const queryElements = {
      origin: getQueryElements(container, 'origin'),
      time: getQueryElements(container, 'time')
    };

    const margin = 60;
    let width = window.innerWidth / 2 - 15 - 2 * margin;
    let height = 300 - 2 * margin;

    if (sockets[progressive]) {
      sockets[progressive].forEach(socket => socket.close(1000, progressive ? '1' : '0'))
    }
    const socket = new WebSocket('ws://' + window.location.hostname + ':8081/' + (progressive ? 'progressive' : 'native'));
    sockets[progressive] = [socket];

    let originChart = null;
    let xScaleOrigin = null;
    let yScaleOrigin = null;

    let timeChart = null;
    let xScaleTime = null;
    let yScaleTime = null;

    let selectedOrigin = null;
    let origins = null;
    let selectedYear = null;
    let selectedMonth = null;
    let selectedDay = null;

    resetProgress('origin');
    resetProgress('time');

    socket.onopen = () => {
      socket.send(JSON.stringify({
        progressive, queryId, view
      }));

      if (view) {
        queryElements.time.container.style.display = 'block';
      }
    };

    socket.onerror = error => {
      console.log('WebSocket Error ' + error);
    };

    socket.onmessage = e => {
      const data = JSON.parse(e.data);

      switch (data.type) {
        case 'origin':
          if (xScaleOrigin) {
            updateOrigin(data.entries);
          } else {
            origins = data.entries.map(e => e.ORIGIN || e.origin);
            drawOrigin(data.entries);
          }
          updateProgress('origin', data.entries);
          break;
        case 'time':
          if ((selectedYear && (data.entries[0].year || data.entries[0].YEAR))) {
            // ignore
          } else {
            if (xScaleTime) {
              updateTime(data.entries);
            } else {
              drawTime(data.entries);
            }
            updateProgress('time', data.entries);
          }
          break;
      }
    };

    function updateProgress(type, data) {
      let progress = (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) || 1;

      queryElements[type].progressBar.style.display = 'none';
      queryElements[type].progressLabel.innerHTML = Math.round(progress * 10000) / 100 + '% processed';
    }

    function resetProgress(type) {
      queryElements[type].chart.style.display = 'none';
      queryElements[type].progressBar.style.display = 'flex';
      queryElements[type].progressLabel.innerHTML = 'Processing...';
    }

    function drawOrigin(data) {
      const svg = queryElements.origin.svg;
      queryElements.origin.chart.style.display = 'block';

      svg.selectAll('g').remove();
      svg.selectAll("text.title").remove();
      svg.selectAll("text.label").remove();

      originChart = svg.append('g')
      .attr('transform', `translate(${margin}, ${margin})`);

      xScaleOrigin = d3.scaleBand()
      .range([0, width])
      .domain(data.map((s) => (s.ORIGIN || s.origin)))
      .padding(0.4);

      const sorted = data.sort((e1, e2) => (e1.depdelay || e1.DEPDELAY) - (e2.depdelay || e2.DEPDELAY));
      const minDelay = Math.min(0, sorted[0].depdelay || sorted[0].DEPDELAY);
      const maxDelay = Math.max(13, Math.ceil(sorted[sorted.length - 1].depdelay || sorted[sorted.length - 1].DEPDELAY));

      yScaleOrigin = d3.scaleLinear()
      .range([height, 0])
      .domain([minDelay, maxDelay]);

      const makeYLines = () => d3.axisLeft()
      .scale(yScaleOrigin);

      originChart.append('g')
      .attr('class', 'axis-x')
      .attr('transform', `translate(0, ${height})`)
      .call(d3.axisBottom(xScaleOrigin))
      .selectAll("text")
      .attr("dx", "-25px")
      .attr("dy", "-.45em")
      .attr("transform", "rotate(-90)");

      originChart.append('g')
      .attr('class', 'axis-y')
      .call(d3.axisLeft(yScaleOrigin));

      originChart.append('g')
      .attr('class', 'grid')
      .call(makeYLines()
          .tickSize(-width, 0, 0)
          .tickFormat('')
      );

      const barGroups = originChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

      barGroups
      .append('rect')
      .attr('class', g => {
        let classes = 'bar';
        if (selectedOrigin && ((g.ORIGIN || g.origin) === (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin))) {
          classes += ' active'
        }
        return classes
      })
      .attr('x', (g) => xScaleOrigin(g.ORIGIN || g.origin))
      .attr('y', (g) => yScaleOrigin(Math.min(g.DEPDELAY || g.depdelay)))
      .attr('height', (g) => height - yScaleOrigin(Math.min(g.DEPDELAY || g.depdelay)))
      .attr('width', xScaleOrigin.bandwidth())
      .on('click', function (data) {
        selectOrigin(this, data)
      });

      if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y1', (g) => yScaleOrigin(Math.min((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence))))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y2', (g) => yScaleOrigin(Math.min((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence))));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
      }

      svg
      .append('text')
      .attr('class', 'label')
      .attr('x', -(height / 2) - margin)
      .attr('y', margin / 2.4)
      .attr('transform', 'rotate(-90)')
      .attr('text-anchor', 'middle')
      .text('Average Delay');

      svg.append('text')
      .attr('class', 'label')
      .attr('x', width / 2 + margin)
      .attr('y', height + margin * 2 - 5)
      .attr('text-anchor', 'middle')
      .text('Airports');

      svg.append('text')
      .attr('class', 'title')
      .attr('x', width / 2 + margin)
      .attr('y', 40)
      .attr('text-anchor', 'middle')
      .text('Average Departure Delay per Airport')
    }

    function updateOrigin(data) {
      originChart.selectAll("g.bar").remove();

      const sorted = data.sort((e1, e2) => (e1.depdelay || e1.DEPDELAY) - (e2.depdelay || e2.DEPDELAY));
      const minDelay = Math.min(0, sorted[0].depdelay || sorted[0].DEPDELAY);
      const maxDelay = Math.max(13, Math.ceil(sorted[sorted.length - 1].depdelay || sorted[sorted.length - 1].DEPDELAY));

      originChart.selectAll(".grid, .axis-y").remove();

      yScaleOrigin = d3.scaleLinear()
      .range([height, 0])
      .domain([minDelay, maxDelay]);

      const makeYLines = () => d3.axisLeft()
      .scale(yScaleOrigin);

      originChart.append('g')
      .attr('class', 'axis-y')
      .call(d3.axisLeft(yScaleOrigin));

      originChart.append('g')
      .attr('class', 'grid')
      .call(makeYLines()
          .tickSize(-width, 0, 0)
          .tickFormat('')
      );

      const barGroups = originChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

      barGroups
      .append('rect')
      .attr('class', g => {
        let classes = 'bar';
        if (selectedOrigin && ((g.ORIGIN || g.origin) === (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin))) {
          classes += ' active'
        }
        return classes
      })
      .attr('x', (g) => xScaleOrigin(g.ORIGIN || g.origin))
      .attr('y', (g) => yScaleOrigin(Math.min(g.DEPDELAY || g.depdelay)))
      .attr('height', (g) => height - yScaleOrigin(Math.min(g.DEPDELAY || g.depdelay)))
      .attr('width', xScaleOrigin.bandwidth())
      .on('click', function (data) {
        selectOrigin(this, data)
      });

      if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y1', (g) => yScaleOrigin(Math.min((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence))))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y2', (g) => yScaleOrigin(Math.min((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence))));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
      }
    }

    function selectOrigin(element, data) {
      queryElements.time.container.style.display = 'block';

      if (selectedOrigin && ((data.ORIGIN || data.origin) === (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin))) {
        selectedOrigin = null;
        originChart.selectAll(".bar.active").classed('active', false);

        const time = [
          (selectedYear || {data: {}}).data.year || (selectedYear || {data: {}}).data.YEAR,
          (selectedMonth || {data: {}}).data.month || (selectedMonth || {data: {}}).data.MONTH,
          (selectedDay || {data: {}}).data.dayofmonth || (selectedDay || {data: {}}).data.DAYOFMONTH,
        ].filter(time => !!time);

        socket.send(JSON.stringify({
          progressive: progressive,
          queryId: queryId,
          view: view,
          time: time.length > 0 ? time : undefined,
          start: 'time',
        }));
      } else if (!selectedOrigin || ((data.ORIGIN || data.origin) !== (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin))) {
        if (selectedOrigin) {
          originChart.selectAll(".bar.active").classed('active', false)
        }
        selectedOrigin = {
          element: element,
          data: data
        };
        element.classList.add('active');

        const time = [
          (selectedYear || {data: {}}).data.year || (selectedYear || {data: {}}).data.YEAR,
          (selectedMonth || {data: {}}).data.month || (selectedMonth || {data: {}}).data.MONTH,
          (selectedDay || {data: {}}).data.dayofmonth || (selectedDay || {data: {}}).data.DAYOFMONTH,
        ].filter(time => !!time);

        socket.send(JSON.stringify({
          progressive: progressive,
          queryId: queryId,
          view: view,
          origin: selectedOrigin.data.origin || selectedOrigin.data.ORIGIN,
          time: time.length > 0 ? time : undefined,
          start: 'time',
        }));
      }

      if (!progressive) {
        resetProgress('time');
        xScaleTime = null;
      }
    }

    function drawTime(data) {
      const svg = queryElements.time.svg;

      svg.selectAll('g').remove();
      svg.selectAll("text.label").remove();
      svg.selectAll("text.title").remove();

      queryElements.time.chart.style.display = 'block';

      xScaleTime = d3.scaleBand()
      .range([0, width])
      .domain(queryId === 0 ? ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] : data.map((s) => (s.YEAR || s.year || s.MONTH || s.month || s.DAYOFMONTH || s.dayofmonth)))
      .padding(0.4);

      const sorted = data.sort((e1, e2) => (e1.depdelay || e1.DEPDELAY) - (e2.depdelay || e2.DEPDELAY));
      const minDelay = Math.min(0, sorted[0].depdelay || sorted[0].DEPDELAY);
      const maxDelay = Math.max(16, Math.ceil(sorted[sorted.length - 1].depdelay || sorted[sorted.length - 1].DEPDELAY));

      svg.on('click', selectTime);

      timeChart = svg.append('g')
        .attr('transform', `translate(${margin}, ${margin})`);

      yScaleTime = d3.scaleLinear()
        .range([height, 0])
        .domain([minDelay, maxDelay]);

      const makeYLines = () => d3.axisLeft()
        .scale(yScaleTime);

      if (queryId === 1 && !selectedYear) {
        timeChart.append('g')
        .attr('class', 'axis-x')
        .attr('transform', `translate(0, ${height})`)
        .call(d3.axisBottom(xScaleTime))
        .selectAll("text")
        .attr("dx", "-25px")
        .attr("dy", "-.45em")
        .attr("transform", "rotate(-90)");
      } else {
        timeChart.append('g')
        .attr('class', 'axis-x')
        .attr('transform', `translate(0, ${height})`)
        .call(d3.axisBottom(xScaleTime))
        .selectAll("text");
      }

      timeChart.append('g')
      .attr('class', 'axis-y')
      .call(d3.axisLeft(yScaleTime));

      timeChart.append('g')
      .attr('class', 'grid')
      .call(makeYLines()
          .tickSize(-width, 0, 0)
          .tickFormat('')
      )
      .on('click', function (data) {
        console.log(this, data)
      });

      const barGroups = timeChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

      barGroups
      .append('rect')
      .attr('class', g => {
        let classes = 'bar';
        if (queryId === 0) {
          if (selectedDay && ((g.DAYOFWEEK || g.dayofweek) === (selectedDay.data.DAYOFWEEK || selectedDay.data.dayofweek))) {
            classes += ' active'
          }
        } else {
          if (selectedDay && ((g.DAYOFMONTH || g.dayofmonth) === (selectedDay.data.DAYOFMONTH || selectedDay.data.dayofmonth))) {
            classes += ' active'
          } else if (selectedMonth && ((g.MONTH || g.month) === (selectedMonth.data.MONTH || selectedMonth.data.month))) {
            classes += ' active'
          } else if (selectedYear && ((g.YEAR || g.year) === (selectedYear.data.YEAR || selectedYear.data.year))) {
            classes += ' active'
          }
        }
        return classes
      })
      .attr('x', (g) => {
        if (queryId === 0) {
          return xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek))
        } else {
          if (selectedMonth || selectedYear) {
            return xScaleTime(g.MONTH || g.month)
          } else {
            return xScaleTime(g.YEAR || g.year)
          }
        }
      })
      .attr('y', (g) => yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => height - yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('width', xScaleTime.bandwidth())
      .on('click', function (data) {
        d3.event.stopPropagation();
        selectTime(this, data)
      });

      if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() / 2)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() / 2)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
      }

      svg
      .append('text')
      .attr('class', 'label')
      .attr('x', -(height / 2) - margin)
      .attr('y', margin / 2.4)
      .attr('transform', 'rotate(-90)')
      .attr('text-anchor', 'middle')
      .text('Average delay');

      svg.append('text')
      .attr('class', 'label')
      .attr('x', width / 2 + margin)
      .attr('y', height + margin * 2 - 5)
      .attr('text-anchor', 'middle')
      .text(getTimeLabel());

      svg.append('text')
      .attr('class', 'title')
      .attr('x', width / 2 + margin)
      .attr('y', 40)
      .attr('text-anchor', 'middle')
      .text(getTimeHeader());
    }

    function updateTime(data) {
      const svg = queryElements.time.svg;

      timeChart.selectAll("g.bar").remove();
      timeChart.selectAll("text.title").remove();

      const sorted = data.sort((e1, e2) => (e1.depdelay || e1.DEPDELAY) - (e2.depdelay || e2.DEPDELAY));
      const minDelay = Math.min(0, sorted[0].depdelay || sorted[0].DEPDELAY);
      const maxDelay = Math.max(16, Math.ceil(sorted[sorted.length - 1].depdelay || sorted[sorted.length - 1].DEPDELAY));

      timeChart.selectAll(".grid, .axis-y").remove();

      yScaleTime = d3.scaleLinear()
      .range([height, 0])
      .domain([minDelay, maxDelay]);

      const makeYLines = () => d3.axisLeft()
      .scale(yScaleTime);

      timeChart.append('g')
      .attr('class', 'axis-y')
      .call(d3.axisLeft(yScaleTime));

      timeChart.append('g')
      .attr('class', 'grid')
      .call(makeYLines()
          .tickSize(-width, 0, 0)
          .tickFormat('')
      );

      svg.on('click', selectTime);

      const barGroups = timeChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

      barGroups
      .append('rect')
      .attr('class', g => {
        let classes = 'bar';

        if (queryId === 0) {
          if (selectedDay && ((g.DAYOFWEEK || g.dayofweek) === (selectedDay.data.DAYOFWEEK || selectedDay.data.dayofweek))) {
            classes += ' active'
          }
        } else {
          if (selectedDay && ((g.DAYOFMONTH || g.dayofmonth) === (selectedDay.data.DAYOFMONTH || selectedDay.data.dayofmonth))) {
            classes += ' active'
          } else if (selectedMonth && ((g.MONTH || g.month) === (selectedMonth.data.MONTH || selectedMonth.data.month))) {
            classes += ' active'
          } else if (selectedYear && ((g.YEAR || g.year) === (selectedYear.data.YEAR || selectedYear.data.year))) {
            classes += ' active'
          }
        }
        return classes
      })
      .attr('x', (g) => getTimeScaling(g))
      .attr('y', (g) => yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => {
        return height - yScaleTime(g.DEPDELAY || g.depdelay);
      })
      .attr('width', xScaleTime.bandwidth())
      .on('click', function (data) {
        d3.event.stopPropagation();
        selectTime(this, data)
      });

      if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() / 2)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() / 2)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

        barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 1 / 4)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => getTimeScaling(g) + xScaleTime.bandwidth() * 3 / 4)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
      }
    }

    function getTimeScaling(g) {
      if (queryId === 0) {
        return xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek))
      } else {
        if (selectedMonth || selectedYear) {
          return xScaleTime(g.MONTH || g.month)
        } else {
          return xScaleTime(g.YEAR || g.year)
        }
      }
    }

    function selectTime(element, data) {
      queryElements.time.container.style.display = 'block';

      if (queryId === 0) {
        if (!data) {
          return
        }
        if (selectedDay && ((data.DAYOFWEEK || data.dayofweek) === (selectedDay.data.DAYOFWEEK || selectedDay.data.dayofweek))) {
          selectedDay = null;
          timeChart.selectAll(".bar.active").classed('active', false);

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            start: 'origin',
          }));
        } else if (!selectedDay || ((data.DAYOFWEEK || data.dayofweek) !== (selectedDay.data.DAYOFWEEK || selectedDay.data.dayofweek))) {
          if (selectedDay) {
            timeChart.selectAll(".bar.active").classed('active', false)
          }
          selectedDay = {
            element: element,
            data: data
          };
          element.classList.add('active');

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            time: [selectedDay.data.dayofweek || selectedDay.data.DAYOFWEEK],
            start: 'origin',
          }));
        }
      } else {
        if (!data && !selectedYear) {
          return
        }

        if (!progressive && !selectedYear) {
          resetProgress('time');
          xScaleTime = null;
        }

        if (!data && selectedYear) {
          selectedMonth = null;
          selectedYear = null;
          xScaleTime = null;

          timeChart.selectAll(".bar.active").classed('active', false);

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            origin: (selectedOrigin || {data: {}}).data.ORIGIN || (selectedOrigin || {data: {}}).data.origin,
            start: null,
          }));
        } else if (selectedMonth && ((data.MONTH || data.month) === (selectedMonth.data.MONTH || selectedMonth.data.month))) {
          selectedMonth = null;
          timeChart.selectAll(".bar.active").classed('active', false);

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            time: [selectedYear.data.year || selectedYear.data.YEAR],
            start: 'origin',
          }));
        } else if (selectedYear && (!selectedMonth || ((data.MONTH || data.month) !== (selectedMonth.data.MONTH || selectedMonth.data.month)))) {
          if (selectedMonth) {
            timeChart.selectAll(".bar.active").classed('active', false)
          }
          selectedMonth = {
            element: element,
            data: data
          };
          xScaleTime = null;

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            time: [
              selectedYear.data.year || selectedYear.data.YEAR,
              selectedMonth.data.month || selectedMonth.data.MONTH
            ],
            start: "origin",
          }));

          element.classList.add('active');
        } else if (!selectedYear) {
          selectedYear = {
            element: element,
            data: data
          };
          xScaleTime = null;

          socket.send(JSON.stringify({
            progressive: progressive,
            queryId: queryId,
            view: view,
            time: [selectedYear.data.year || selectedYear.data.YEAR],
            origin: (selectedOrigin || {data: {}}).data.ORIGIN || (selectedOrigin || {data: {}}).data.origin,
            start: null,
          }));
        }
      }

      if (!progressive) {
        resetProgress('origin');
        xScaleOrigin = null;
      }
    }

    function getTimeHeader() {
      if (queryId === 0) {
        return 'Average Departure Delay per Weekday'
      } else {
        if (selectedYear) {
          return 'Average Departure Delay per Month of Year ' + (selectedYear.data.year || selectedYear.data.YEAR)
        } else {
          return 'Average Departure Delay per Year'
        }
      }
    }

    function getTimeLabel() {
      if (queryId === 0) {
        return 'Weekdays'
      } else {
        if (selectedMonth) {
          return 'Days'
        } else if (selectedYear) {
          return 'Months'
        } else {
          return 'Years'
        }
      }
    }
  };

  window.query = (queryId, view) => {
    start(true, queryId, view);
    start(false, queryId, view);
  }
});