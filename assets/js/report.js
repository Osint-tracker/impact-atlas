// Report Generation Engine (Client-Side)
(function () {
    const EVENTS_DATA_URL = 'assets/data/events.geojson';
    const EMPTY_EXECUTIVE_SUMMARY = 'NO DATA AVAILABLE FOR SELECTED PERIOD. AWAITING NEW INTERCEPTS.';
    const EMPTY_STRATEGIC_OUTLOOK = 'INSUFFICIENT KINETIC DATA TO GENERATE OUTLOOK.';
    const ACTION_BUTTON_IDS = ['generateDetailedReportBtn', 'downloadTextBtn', 'downloadPdfBtn'];

    let threatChartInstance = null;
    window.currentBriefingModel = null;

    document.addEventListener('DOMContentLoaded', async function () {
        bindReportActions();
        startClock();

        const urlParams = new URLSearchParams(window.location.search);
        const startDateStr = urlParams.get('start');
        const endDateStr = urlParams.get('end') || 'LIVE';
        const startDate = parseDateParam(startDateStr, false);
        const endDate = endDateStr === 'LIVE' ? new Date() : parseDateParam(endDateStr, true);
        const dateLabel = buildDateLabel(startDate, endDate, endDateStr);

        setText('.date', dateLabel);

        if (!startDate || !endDate) {
            renderBriefing(buildEmptyBriefingModel(dateLabel));
            return;
        }

        try {
            const response = await fetch(EVENTS_DATA_URL);
            if (!response.ok) {
                throw new Error('HTTP ' + response.status);
            }

            const payload = await response.json();
            const features = Array.isArray(payload && payload.features) ? payload.features : [];
            const events = features
                .map(normalizeFeature)
                .filter(function (event) {
                    return event.dateObj && event.dateObj >= startDate && event.dateObj <= endDate;
                });

            renderBriefing(buildBriefingModel(events, dateLabel));
        } catch (error) {
            console.error('Failed to load report data:', error);
            renderBriefing(buildEmptyBriefingModel(dateLabel));
        }
    });

    function bindReportActions() {
        const textButton = document.getElementById('downloadTextBtn');
        if (textButton) textButton.addEventListener('click', downloadBriefingText);

        const pdfButton = document.getElementById('downloadPdfBtn');
        if (pdfButton) pdfButton.addEventListener('click', printBriefingCard);

        const detailedButton = document.getElementById('generateDetailedReportBtn');
        if (detailedButton) detailedButton.addEventListener('click', printBriefingCard);

        window.addEventListener('afterprint', function () {
            document.body.classList.remove('print-briefing-only');
        });
    }

    function startClock() {
        const clock = document.getElementById('utc-time');
        if (!clock) return;

        const tick = function () {
            clock.innerText = new Date().toISOString().split('T')[1].split('.')[0] + ' UTC';
        };

        tick();
        setInterval(tick, 1000);
    }

    function parseDateParam(value, endOfDay) {
        if (!value) return null;

        const raw = String(value).trim();
        const dateOnly = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (dateOnly) {
            return new Date(
                Number(dateOnly[1]),
                Number(dateOnly[2]) - 1,
                Number(dateOnly[3]),
                endOfDay ? 23 : 0,
                endOfDay ? 59 : 0,
                endOfDay ? 59 : 0,
                endOfDay ? 999 : 0
            );
        }

        const parsed = new Date(raw);
        return Number.isFinite(parsed.getTime()) ? parsed : null;
    }

    function parseEventDateValue(value) {
        if (value == null || value === '') return null;

        if (typeof value === 'number' || /^\d+$/.test(String(value).trim())) {
            let numeric = Number(value);
            if (!Number.isFinite(numeric) || numeric <= 0) return null;
            if (numeric < 100000000000) numeric *= 1000;
            const byNumber = new Date(numeric);
            return byNumber.getFullYear() >= 2000 ? byNumber : null;
        }

        const raw = String(value).trim();
        const dateOnly = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (dateOnly) {
            return new Date(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3]));
        }

        const dmy = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
        if (dmy) {
            let year = Number(dmy[3]);
            if (year < 100) year += 2000;
            return new Date(year, Number(dmy[2]) - 1, Number(dmy[1]), Number(dmy[4] || 0), Number(dmy[5] || 0), Number(dmy[6] || 0));
        }

        const normalized = raw.indexOf('T') === -1 && raw.indexOf(' ') > -1 ? raw.replace(' ', 'T') : raw;
        const byString = new Date(normalized);
        return Number.isFinite(byString.getTime()) && byString.getFullYear() >= 2000 ? byString : null;
    }

    function getEventDate(props) {
        return parseEventDateValue(props.date)
            || parseEventDateValue(props.timestamp)
            || parseEventDateValue(props.event_date)
            || parseEventDateValue(props.last_seen_date)
            || parseEventDateValue(props.created_at);
    }

    function normalizeFeature(feature) {
        const props = feature && feature.properties ? feature.properties : (feature || {});
        const coords = feature && feature.geometry && Array.isArray(feature.geometry.coordinates)
            ? feature.geometry.coordinates
            : [];

        return {
            id: safeString(props.id || props.event_id || props.cluster_id, 'UNKNOWN_EVENT'),
            title: safeString(props.title || props.event_title || props.description, 'Untitled event'),
            description: safeString(props.description || props.ai_reasoning || '', ''),
            dateObj: getEventDate(props),
            rawDate: safeString(props.date || props.timestamp || props.event_date || props.last_seen_date, ''),
            tie: numberOrDefault(props.tie_total != null ? props.tie_total : props.tie_score, 0),
            vecK: getNumber(props.vec_k != null ? props.vec_k : props.kinetic_score),
            vecT: getNumber(props.vec_t != null ? props.vec_t : props.target_score),
            vecE: getNumber(props.vec_e != null ? props.vec_e : props.effect_score),
            reliability: getNumber(props.reliability != null ? props.reliability : props.source_reputation_score),
            classification: normalizeLabel(props.classification || props.category || props.event_category, 'UNKNOWN'),
            operationalSector: normalizeLabel(props.operational_sector || props.sector || props.region, 'UNKNOWN'),
            campaignName: normalizeLabel(props.campaign_name || props.campaign_id, ''),
            faction: normalizeLabel(props.faction || props.actor || props.side, 'UNKNOWN'),
            targetType: normalizeLabel(props.target_type || props.target || '', ''),
            sources: parseSources(props.sources_list || props.source || props.sources),
            lat: getNumber(coords[1]),
            lon: getNumber(coords[0])
        };
    }

    function buildBriefingModel(events, dateLabel) {
        if (!Array.isArray(events) || events.length === 0) {
            return buildEmptyBriefingModel(dateLabel);
        }

        const sortedByTie = events.slice().sort(function (a, b) {
            return b.tie - a.tie;
        });
        const topEvent = sortedByTie[0];
        const topAlerts = sortedByTie.slice(0, 3);
        const topClassifications = topCounts(events, function (event) {
            return event.classification;
        }, 3);
        const topSectors = topCounts(events, function (event) {
            return event.operationalSector;
        }, 1);
        const avgTie = average(events.map(function (event) { return event.tie; }));
        const avgK = averageDefined(events.map(function (event) { return event.vecK; }));
        const avgT = averageDefined(events.map(function (event) { return event.vecT; }));
        const avgE = averageDefined(events.map(function (event) { return event.vecE; }));
        const sourceCount = countUniqueSources(events);
        const dominantSector = topSectors.length ? topSectors[0].label : 'UNKNOWN';
        const dominantClassification = topClassifications.length ? topClassifications[0].label : 'UNKNOWN';
        const classificationMix = topClassifications.length
            ? topClassifications.map(function (item) { return item.label + ' (' + item.count + ')'; }).join(', ')
            : 'UNKNOWN';
        const vectorText = formatVector(avgK, avgT, avgE);
        const topEventVectorText = formatVector(topEvent.vecK, topEvent.vecT, topEvent.vecE);

        const summaryText = [
            'Executive Summary: Selected period contains ' + events.length + ' parsed OSINT events. Dominant operational sector: ' + dominantSector + '. Average T.I.E. score: ' + formatNumber(avgTie) + '.',
            'Primary AI classification: ' + dominantClassification + '. Top classifications: ' + classificationMix + '. Highest T.I.E. event: "' + topEvent.title + '" (' + formatNumber(topEvent.tie) + ') in ' + topEvent.operationalSector + '.'
        ].join('\n\n');

        const outlookText = [
            'Strategic Outlook: Current period signal is concentrated in ' + dominantSector + ', led by ' + dominantClassification + ' activity across ' + events.length + ' events.',
            'Highest-impact event: "' + topEvent.title + '" with T.I.E. ' + formatNumber(topEvent.tie) + ' and K/T/E vector ' + topEventVectorText + '. Period average vector: ' + vectorText + '.',
            'Observed classification mix: ' + classificationMix + '. This outlook is limited to parsed GeoJSON records for the selected period.'
        ].join('\n\n');

        return {
            hasData: true,
            dateLabel: dateLabel,
            generatedDate: new Date().toISOString().substring(0, 10),
            events: events,
            eventCount: events.length,
            avgTie: avgTie,
            avgK: avgK,
            avgT: avgT,
            avgE: avgE,
            dominantSector: dominantSector,
            dominantClassification: dominantClassification,
            topClassifications: topClassifications,
            topAlerts: topAlerts,
            sourceCount: sourceCount,
            topEvent: topEvent,
            summaryText: summaryText,
            outlookText: outlookText
        };
    }

    function buildEmptyBriefingModel(dateLabel) {
        return {
            hasData: false,
            dateLabel: dateLabel || 'UNSPECIFIED PERIOD',
            generatedDate: new Date().toISOString().substring(0, 10),
            events: [],
            eventCount: 0,
            avgTie: 0,
            avgK: null,
            avgT: null,
            avgE: null,
            dominantSector: 'UNKNOWN',
            dominantClassification: 'UNKNOWN',
            topClassifications: [],
            topAlerts: [],
            sourceCount: 0,
            topEvent: null,
            summaryText: EMPTY_EXECUTIVE_SUMMARY,
            outlookText: EMPTY_STRATEGIC_OUTLOOK
        };
    }

    function renderBriefing(model) {
        window.currentBriefingModel = model;

        renderMetrics(model);
        renderTrendChart(model.events);
        renderExecutiveSummary(model);
        renderStrategicOutlook(model);
        renderTopAlerts(model);
        setActionButtonsEnabled(model.hasData);
    }

    function renderMetrics(model) {
        setText('#metricEvents', String(model.eventCount));
        setText('#metricTie', formatNumber(model.avgTie));
        setText('#metricSources', String(model.sourceCount));
        setText('#metricSector', model.dominantSector);

        const eventsDelta = document.getElementById('metricEventsDelta');
        if (eventsDelta) {
            eventsDelta.innerHTML = model.hasData
                ? '<span>' + model.topClassifications.length + ' CLASS TYPES</span>'
                : '<span>--</span>';
        }

        const tieIcon = document.getElementById('metricTieIcon');
        const tieLabel = document.getElementById('metricTieLabel');
        const level = getTieLevel(model.avgTie);

        if (tieIcon) tieIcon.style.color = level.color;
        if (tieLabel) {
            tieLabel.innerHTML = model.hasData
                ? 'Warning Level: <span style="color:' + level.color + '">' + level.label + '</span>'
                : 'NO DATA';
        }

        const sectorLabel = document.getElementById('metricSectorLabel');
        if (sectorLabel) {
            sectorLabel.innerHTML = model.hasData
                ? '<i class="fa-solid fa-crosshairs"></i> DATA-BACKED ACTIVE SECTOR'
                : 'NO DATA';
        }
    }

    function renderExecutiveSummary(model) {
        const container = document.getElementById('execSummaryText');
        const badge = document.getElementById('alertBadge');
        if (!container) return;

        if (!model.hasData) {
            container.innerHTML = '<p class="briefing-empty-text">' + escapeHtml(EMPTY_EXECUTIVE_SUMMARY) + '</p>';
            if (badge) badge.style.display = 'none';
            return;
        }

        const classificationMix = formatCountList(model.topClassifications);
        const topEvent = model.topEvent;

        container.innerHTML = [
            '<p><b>Executive Summary:</b> Selected period contains <b>' + model.eventCount + '</b> parsed OSINT events. ' +
            'Dominant operational sector: <b>' + escapeHtml(model.dominantSector) + '</b>. ' +
            'Average T.I.E. score: <b>' + formatNumber(model.avgTie) + '</b>.</p>',
            '<p>Primary AI classification: <b>' + escapeHtml(model.dominantClassification) + '</b>. ' +
            'Top classifications: <b>' + escapeHtml(classificationMix) + '</b>. ' +
            'Highest T.I.E. event: "<b>' + escapeHtml(topEvent.title) + '</b>" ' +
            '(<b>' + formatNumber(topEvent.tie) + '</b>) in <b>' + escapeHtml(topEvent.operationalSector) + '</b>.</p>'
        ].join('');

        if (badge) badge.style.display = model.avgTie >= 70 || topEvent.tie >= 80 ? 'flex' : 'none';
    }

    function renderStrategicOutlook(model) {
        const container = document.getElementById('strategicOutlookText');
        if (!container) return;

        if (!model.hasData) {
            container.innerHTML = '<p class="briefing-empty-text">' + escapeHtml(EMPTY_STRATEGIC_OUTLOOK) + '</p>';
            return;
        }

        const classificationMix = formatCountList(model.topClassifications);
        const topEvent = model.topEvent;

        container.innerHTML = [
            '<p><b>Strategic Outlook:</b> Current period signal is concentrated in <b>' + escapeHtml(model.dominantSector) + '</b>, ' +
            'led by <b>' + escapeHtml(model.dominantClassification) + '</b> activity across <b>' + model.eventCount + '</b> events.</p>',
            '<p>Highest-impact event: "<b>' + escapeHtml(topEvent.title) + '</b>" with T.I.E. <b>' + formatNumber(topEvent.tie) + '</b> ' +
            'and K/T/E vector <b>' + escapeHtml(formatVector(topEvent.vecK, topEvent.vecT, topEvent.vecE)) + '</b>. ' +
            'Period average vector: <b>' + escapeHtml(formatVector(model.avgK, model.avgT, model.avgE)) + '</b>.</p>',
            '<p>Observed classification mix: <b>' + escapeHtml(classificationMix) + '</b>. This outlook is limited to parsed GeoJSON records for the selected period.</p>'
        ].join('');
    }

    function renderTopAlerts(model) {
        const list = document.getElementById('topAlertsList') || document.querySelector('.alerts-list');
        if (!list) return;

        if (!model.hasData || model.topAlerts.length === 0) {
            list.innerHTML = '<div class="briefing-empty-row">No high-priority alerts for this period.</div>';
            return;
        }

        list.innerHTML = model.topAlerts.map(function (event) {
            const badgeClass = event.tie >= 80 ? 'red' : event.tie >= 50 ? 'amber' : 'orange';
            const tagClass = event.tie >= 80 ? 'ugent' : event.tie >= 50 ? 'warning' : 'monitor';
            const dateText = event.dateObj ? event.dateObj.toISOString().substring(0, 10) : 'UNKNOWN';
            const description = event.description || event.campaignName || event.targetType || 'No event description provided by source data.';

            return '<div class="alert-item">' +
                '<div class="score-badge ' + badgeClass + '">' + Math.round(event.tie) + '</div>' +
                '<div class="alert-info">' +
                '<div class="alert-title">' + escapeHtml(event.title) + '<span class="tag ' + tagClass + '">' + escapeHtml(event.classification) + '</span></div>' +
                '<div class="alert-desc">' + escapeHtml(event.operationalSector) + ' | ' + escapeHtml(truncateText(description, 180)) + '</div>' +
                '</div>' +
                '<div class="alert-meta">' + escapeHtml(dateText) + '<br>TIE ' + formatNumber(event.tie) + '</div>' +
                '</div>';
        }).join('');
    }

    function renderTrendChart(events) {
        const canvas = document.getElementById('threatChart');
        if (!canvas || typeof Chart === 'undefined') return;

        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        const dateCounts = {};
        (Array.isArray(events) ? events : []).forEach(function (event) {
            if (!event.dateObj) return;
            const day = event.dateObj.toISOString().substring(0, 10);
            dateCounts[day] = (dateCounts[day] || 0) + event.tie;
        });

        const labels = Object.keys(dateCounts).sort();
        const dataPoints = labels.map(function (day) { return dateCounts[day]; });

        if (labels.length === 0) {
            labels.push('No Data');
            dataPoints.push(0);
        }

        if (threatChartInstance) {
            threatChartInstance.destroy();
        }

        const gradient = ctx.createLinearGradient(0, 0, 0, 400);
        gradient.addColorStop(0, 'rgba(239, 68, 68, 0.5)');
        gradient.addColorStop(1, 'rgba(239, 68, 68, 0.0)');

        threatChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Cumulative Threat Score',
                    data: dataPoints,
                    borderColor: '#ef4444',
                    backgroundColor: gradient,
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true,
                    pointBackgroundColor: '#ef4444',
                    pointRadius: 3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: {
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        ticks: { color: '#94a3b8', maxTicksLimit: 6 }
                    },
                    y: { display: false }
                }
            }
        });
    }

    function setActionButtonsEnabled(enabled) {
        ACTION_BUTTON_IDS.forEach(function (id) {
            const button = document.getElementById(id);
            if (!button) return;
            button.disabled = !enabled;
            button.classList.toggle('is-disabled', !enabled);
            button.setAttribute('aria-disabled', enabled ? 'false' : 'true');
        });
    }

    function downloadBriefingText() {
        const model = window.currentBriefingModel;
        if (!model || !model.hasData) return;

        const text = buildBriefingText(model);
        const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = 'Impact_Atlas_Briefing_' + model.generatedDate + '.txt';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
    }

    function printBriefingCard() {
        const model = window.currentBriefingModel;
        if (!model || !model.hasData) return;

        document.body.classList.add('print-briefing-only');
        window.print();
        setTimeout(function () {
            document.body.classList.remove('print-briefing-only');
        }, 1000);
    }

    function buildBriefingText(model) {
        const alertLines = model.topAlerts.length
            ? model.topAlerts.map(function (event, index) {
                return (index + 1) + '. [' + formatNumber(event.tie) + '] ' + event.title + ' | ' + event.operationalSector + ' | ' + event.classification;
            }).join('\n')
            : 'No high-priority alerts for this period.';

        return [
            'IMPACT ATLAS DAILY INTELLIGENCE BRIEFING',
            'Period: ' + model.dateLabel,
            'Generated: ' + new Date().toISOString(),
            '',
            'EXECUTIVE SUMMARY',
            model.summaryText,
            '',
            'TOP ALERTS',
            alertLines,
            '',
            'STRATEGIC OUTLOOK',
            model.outlookText
        ].join('\n');
    }

    function parseSources(value) {
        if (Array.isArray(value)) return value.filter(Boolean);
        if (!value) return [];
        if (typeof value !== 'string') return [];

        const raw = value.trim();
        if (!raw) return [];

        try {
            const parsed = JSON.parse(raw);
            return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
        } catch (error) {
            try {
                const parsed = JSON.parse(raw.replace(/'/g, '"'));
                return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
            } catch (fallbackError) {
                return raw ? [{ name: raw }] : [];
            }
        }
    }

    function countUniqueSources(events) {
        const seen = new Set();
        events.forEach(function (event) {
            event.sources.forEach(function (source) {
                const key = safeString(source && (source.url || source.name || source.source || source.link), '').toLowerCase();
                if (key) seen.add(key);
            });
        });
        return seen.size;
    }

    function topCounts(events, getter, limit) {
        const counts = {};
        events.forEach(function (event) {
            const value = normalizeLabel(getter(event), '');
            if (!value || isUnknownValue(value)) return;
            counts[value] = (counts[value] || 0) + 1;
        });

        return Object.keys(counts)
            .sort(function (a, b) {
                return counts[b] - counts[a] || a.localeCompare(b);
            })
            .slice(0, limit)
            .map(function (label) {
                return { label: label, count: counts[label] };
            });
    }

    function average(values) {
        const clean = values.map(function (value) {
            return Number.isFinite(value) ? value : 0;
        });
        if (!clean.length) return 0;
        return clean.reduce(function (sum, value) { return sum + value; }, 0) / clean.length;
    }

    function averageDefined(values) {
        const clean = values.filter(function (value) {
            return Number.isFinite(value);
        });
        if (!clean.length) return null;
        return clean.reduce(function (sum, value) { return sum + value; }, 0) / clean.length;
    }

    function getTieLevel(avgTie) {
        if (avgTie >= 70) return { label: 'CRITICAL', color: '#ef4444' };
        if (avgTie >= 40) return { label: 'ELEVATED', color: '#f59e0b' };
        return { label: 'NORMAL', color: '#3b82f6' };
    }

    function formatCountList(items) {
        return items && items.length
            ? items.map(function (item) { return item.label + ' (' + item.count + ')'; }).join(', ')
            : 'UNKNOWN';
    }

    function formatVector(k, t, e) {
        return [k, t, e].map(function (value) {
            return Number.isFinite(value) ? formatNumber(value) : 'N/A';
        }).join('/');
    }

    function formatNumber(value) {
        return Number.isFinite(value) ? value.toFixed(1) : '0.0';
    }

    function buildDateLabel(startDate, endDate, endDateStr) {
        const start = startDate ? startDate.toLocaleDateString() : 'UNSPECIFIED';
        const end = endDateStr === 'LIVE' ? 'LIVE' : (endDate ? endDate.toLocaleDateString() : 'UNSPECIFIED');
        return start + ' - ' + end;
    }

    function getNumber(value) {
        const number = parseFloat(value);
        return Number.isFinite(number) ? number : null;
    }

    function numberOrDefault(value, fallback) {
        const number = getNumber(value);
        return number == null ? fallback : number;
    }

    function normalizeLabel(value, fallback) {
        const label = safeString(value, '').trim();
        if (!label) return fallback;
        return isUnknownValue(label) ? fallback : label;
    }

    function isUnknownValue(value) {
        const normalized = String(value || '').trim().toUpperCase();
        return normalized === 'UNKNOWN'
            || normalized === 'UNKNOWN_SECTOR'
            || normalized === 'NULL'
            || normalized === 'N/A'
            || normalized === 'NONE';
    }

    function safeString(value, fallback) {
        if (value == null) return fallback;
        const text = String(value).trim();
        return text || fallback;
    }

    function truncateText(value, maxLength) {
        const text = safeString(value, '');
        if (text.length <= maxLength) return text;
        return text.substring(0, maxLength - 3) + '...';
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function setText(selector, value) {
        const element = selector.charAt(0) === '#'
            ? document.getElementById(selector.substring(1))
            : document.querySelector(selector);
        if (element) element.innerText = value;
    }

    window.downloadBriefingText = downloadBriefingText;
    window.printBriefingCard = printBriefingCard;
})();
