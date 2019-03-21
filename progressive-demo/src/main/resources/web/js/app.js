import * as d3 from "d3";

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

window.start = (progressive, view) => {
  const container = document.getElementById(progressive ? 'progressive' : 'native');

  const queryElements = {
    origin: getQueryElements(container, 'origin'),
    time: getQueryElements(container, 'time')
  };

  const margin = 60;
  let width;
  if (view) {
    width = window.innerWidth - 15 - 2 * margin;
  } else {
    width = window.innerWidth / 2 - 15 - 2 * margin;
  }
  let height = 300 - 2 * margin;

  const socket = new WebSocket('ws://' + window.location.hostname + ':8081');

  let originChart = null;
  let xScaleOrigin = null;
  let yScaleOrigin = null;

  let timeChart = null;
  let xScaleTime = null;
  let yScaleTime = null;

  let selectedOrigin = null;
  let origins = null;

  socket.onopen = () => {
    socket.send(JSON.stringify({
      id: view ? 10 : 0,
      sql: 'select ' + (progressive ? 'progressive' : '') + ' origin, avg(depdelay) depdelay' + (progressive ? ', progressive_partition(), progressive_progress(), progressive_confidence(depdelay)' : '') + ' from ontime1m group by origin',
      progressive: progressive
    }));

    if (view) {
      queryElements.time.container.style.display = 'block';

    //  const where = origins.map(o => 'origin = \'' + o + '\' future').join(' or ');
      const where = '';

      socket.send(JSON.stringify({
        id: view ? 11 : 1,
        sql: 'select progressive dayofweek, depdelay, progressive_partition, progressive_progress, progressive_confidence from v group by dayofweek',
        'sql-display': 'select progressive dayofweek, depdelay, progressive_partition, progressive_progress, progressive_confidence from v',
        'sql-init': 'create progressive view v as select origin, dayofweek, avg(depdelay) depdelay, progressive_partition(), progressive_progress(), progressive_confidence(depdelay) from ontime1m group by origin, dayofweek',
        'sql-init-display': 'create progressive view v as select origin, dayofweek, avg(depdelay) depdelay, progressive_partition(), progressive_progress(), progressive_confidence(depdelay) from ontime1m where ' + where + ' group by dayofweek',
        progressive: progressive
      }));
    }
  };

  socket.onerror = error => {
    console.log('WebSocket Error ' + error);
  };

  socket.onmessage = e => {
    const data = JSON.parse(e.data);

    switch (data.id) {
      case 0:
      case 10:
        if (xScaleOrigin) {
          updateOrigin(data.entries);
        } else {
          origins = data.entries.map(e => e.ORIGIN || e.origin);
          drawOrigin(data.entries);
        }
        updateProgress('origin', data.entries);
        break;
      case 1:
      case 11:
        if (xScaleTime) {
          updateTime(data.entries);
        } else {
          drawTime(data.entries);
        }
        updateProgress('time', data.entries);
        break;
    }
  };

  function updateProgress(type, data) {
    let progress = (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) || 1;

    queryElements[type].progressBar.style.display = 'none';
    queryElements[type].progressLabel.innerHTML = Math.round(progress * 10000) / 100 + '% processed';
  }

  function drawOrigin(data) {
    queryElements.origin.chart.style.display = 'block';

    const svg = queryElements.origin.svg;
    originChart = svg.append('g')
      .attr('transform', `translate(${margin}, ${margin})`);

    xScaleOrigin = d3.scaleBand()
      .range([0, width])
      .domain(data.map((s) => (s.ORIGIN || s.origin)))
      .padding(0.4);

    yScaleOrigin = d3.scaleLinear()
      .range([height, 0])
      .domain([0, 13]);

    console.log('yScaleOrigin', yScaleOrigin);

    const makeYLines = () => d3.axisLeft()
      .scale(yScaleOrigin);

    originChart.append('g')
      .attr('transform', `translate(0, ${height})`)
      .call(d3.axisBottom(xScaleOrigin))
      .selectAll("text")
      .attr("dx", "-25px")
      .attr("dy", "-.45em")
      .attr("transform", "rotate(-90)");

    originChart.append('g')
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
      .attr('class', 'bar')
      .attr('x', (g) => xScaleOrigin(g.ORIGIN || g.origin))
      .attr('y', (g) => yScaleOrigin(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => {
        return height - yScaleOrigin(g.DEPDELAY || g.depdelay);
      })
      .attr('width', xScaleOrigin.bandwidth())
      .on('click', function(data) {
        selectOrigin(this, data)
      });

    if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + 3)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() - 3)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + 3)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() - 3)
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
      .attr('y', (g) => yScaleOrigin(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => {
        return height - yScaleOrigin(g.DEPDELAY || g.depdelay);
      })
      .attr('width', xScaleOrigin.bandwidth())
      .on('click', function(data) {
        selectOrigin(this, data)
      });

    if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() / 2)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + 3)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() - 3)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleOrigin(g.ORIGIN || g.origin) + 3)
        .attr('y1', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleOrigin(g.ORIGIN || g.origin) + xScaleOrigin.bandwidth() - 3)
        .attr('y2', (g) => yScaleOrigin((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
    }
  }

  function selectOrigin(element, data) {
    queryElements.time.container.style.display = 'block';

    if (!selectedOrigin || ((data.ORIGIN || data.origin) !== (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin))) {
      const createView = !selectedOrigin;
      if (selectedOrigin) {
        originChart.selectAll(".bar.active").classed('active', false)
      }
      selectedOrigin = {
        element: element,
        data: data
      };
      element.classList.add('active');

      if (view) {
        socket.send(JSON.stringify({
          id: view ? 11 : 1,
          sql: 'select progressive dayofweek, depdelay, progressive_partition, progressive_progress, progressive_confidence from v where f0 = \'' + (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin) + '\' group by dayofweek',
          'sql-display': 'select progressive dayofweek, depdelay, progressive_partition, progressive_progress, progressive_confidence from v with future where origin = \'' + (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin) + '\'',
          progressive: progressive
        }));
      } else {
        socket.send(JSON.stringify({
          id: view ? 11 : 1,
          sql: 'select ' + (progressive ? 'progressive' : '') + ' dayofweek, avg(depdelay) depdelay' + (progressive ? ', progressive_partition(), progressive_progress(), progressive_confidence(depdelay)' : '') + ' from ontime1m where origin = \'' + (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin) + '\' group by dayofweek',
          progressive: progressive
        }));
      }
    }
  }

  function drawTime(data) {
    queryElements.time.chart.style.display = 'block';

    const svg = queryElements.time.svg;
    timeChart = svg.append('g')
      .attr('transform', `translate(${margin}, ${margin})`);

    xScaleTime = d3.scaleBand()
      .range([0, width])
      .domain(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
      .padding(0.4);

    yScaleTime = d3.scaleLinear()
      .range([height, 0])
      .domain([0, 16]);

    const makeYLines = () => d3.axisLeft()
      .scale(yScaleTime);

    timeChart.append('g')
      .attr('transform', `translate(0, ${height})`)
      .call(d3.axisBottom(xScaleTime))
      .selectAll("text");

    timeChart.append('g')
      .call(d3.axisLeft(yScaleTime));

    timeChart.append('g')
      .attr('class', 'grid')
      .call(makeYLines()
        .tickSize(-width, 0, 0)
        .tickFormat('')
      );

    const barGroups = timeChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

    barGroups
      .append('rect')
      .attr('class', 'bar')
      .attr('x', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)))
      .attr('y', (g) => yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => height - yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('width', xScaleTime.bandwidth());

    if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() / 2)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() / 2)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + 16)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() - 16)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + 16)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() - 16)
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
      .text('Weekdays');

    svg.append('text')
      .attr('class', 'title')
      .attr('x', width / 2 + margin)
      .attr('y', 40)
      .attr('text-anchor', 'middle')
      .text('Average Departure Delay per Weekday' + (selectedOrigin ? ' of ' + (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin) : ''));
  }

  function updateTime(data) {
    const svg = queryElements.time.svg;

    timeChart.selectAll("g.bar").remove();
    svg.selectAll("text.title").remove();

    const barGroups = timeChart.selectAll()
      .data(data)
      .enter()
      .append('g')
      .attr('class', 'bar');

    barGroups
      .append('rect')
      .attr('class', 'bar')
      .attr('x', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)))
      .attr('y', (g) => yScaleTime(g.DEPDELAY || g.depdelay))
      .attr('height', (g) => {
        return height - yScaleTime(g.DEPDELAY || g.depdelay);
      })
      .attr('width', xScaleTime.bandwidth());

    if (progressive && (data[0].PROGRESSIVE_PROGRESS || data[0].progressive_progress) < 1) {
      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() / 2)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() / 2)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + 16)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() - 16)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) + (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));

      barGroups.append('line')
        .style('stroke', errorBarColor)
        .attr('x1', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + 16)
        .attr('y1', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)))
        .attr('x2', (g) => xScaleTime(dayOfWeekToString(g.DAYOFWEEK || g.dayofweek)) + xScaleTime.bandwidth() - 16)
        .attr('y2', (g) => yScaleTime((g.DEPDELAY || g.depdelay) - (g.PROGRESSIVE_CONFIDENCE || g.progressive_confidence)));
    }

    svg.append('text')
      .attr('class', 'title')
      .attr('x', width / 2 + margin)
      .attr('y', 40)
      .attr('text-anchor', 'middle')
      .text('Average Departure Delay per Weekday' + (selectedOrigin ? ' of ' + (selectedOrigin.data.ORIGIN || selectedOrigin.data.origin) : ''));
  }
};
