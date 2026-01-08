// Initializing Chart.js for the Executive Summary

document.addEventListener('DOMContentLoaded', function () {
    const ctx = document.getElementById('threatChart').getContext('2d');

    // Gradient Fill
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(239, 68, 68, 0.5)'); // Red
    gradient.addColorStop(1, 'rgba(239, 68, 68, 0.0)');

    const threatChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', 'NOW'],
            datasets: [{
                label: 'Threat Level',
                data: [45, 48, 52, 65, 78, 85, 92], // Escalating trend
                borderColor: '#ef4444',
                backgroundColor: gradient,
                borderWidth: 2,
                tension: 0.4,
                fill: true,
                pointBackgroundColor: '#ef4444',
                pointBorderColor: '#fff',
                pointRadius: 4,
                pointHoverRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    ticks: {
                        color: '#94a3b8',
                        font: {
                            family: 'JetBrains Mono',
                            size: 10
                        }
                    }
                },
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    ticks: {
                        display: false // Hide Y axis numbers for cleaner look
                    },
                    border: {
                        display: false
                    }
                }
            }
        }
    });

    // Update time continuously
    function updateTime() {
        const now = new Date();
        const timeString = now.toISOString().split('T')[1].split('.')[0] + ' UTC';
        document.getElementById('utc-time').innerText = timeString;
    }
    setInterval(updateTime, 1000);
    updateTime();
});
