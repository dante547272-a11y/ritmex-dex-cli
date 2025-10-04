class FundingMonitor {
    constructor() {
        this.ws = null;
        this.data = { rows: [], spreads: [], status: {}, lastUpdated: null };
        this.currentSort = { column: 'symbol', direction: 'asc' };
        this.enabledExchanges = new Set(['lighter', 'binance', 'hyperliquid', 'edgex', 'grvt', 'aster', 'backpack']);
        this.displayLimit = 20;
        this.capitalAmount = 10000;
        
        this.initializeElements();
        this.bindEvents();
        this.connectWebSocket();
    }

    initializeElements() {
        this.elements = {
            connectionStatus: document.getElementById('connectionStatus'),
            lastUpdated: document.getElementById('lastUpdated'),
            exchangeFilters: document.getElementById('exchangeFilters'),
            sortColumn: document.getElementById('sortColumn'),
            sortDirection: document.getElementById('sortDirection'),
            displayLimit: document.getElementById('displayLimit'),
            capitalAmount: document.getElementById('capitalAmount'),
            statusPanel: document.getElementById('statusPanel'),
            fundingTable: document.getElementById('fundingTable'),
            fundingTableBody: document.getElementById('fundingTableBody'),
            spreadsGrid: document.getElementById('spreadsGrid')
        };
    }

    bindEvents() {
        // 交易所筛选
        this.elements.exchangeFilters.addEventListener('change', (e) => {
            const exchange = e.target.value;
            if (e.target.checked) {
                this.enabledExchanges.add(exchange);
            } else {
                this.enabledExchanges.delete(exchange);
            }
            this.sendFilterUpdate();
            this.updateDisplay();
        });

        // 排序控制
        this.elements.sortColumn.addEventListener('change', (e) => {
            this.currentSort.column = e.target.value;
            this.updateDisplay();
        });

        this.elements.sortDirection.addEventListener('change', (e) => {
            this.currentSort.direction = e.target.value;
            this.updateDisplay();
        });

        // 显示设置
        this.elements.displayLimit.addEventListener('change', (e) => {
            this.displayLimit = parseInt(e.target.value);
            this.updateDisplay();
        });

        this.elements.capitalAmount.addEventListener('change', (e) => {
            this.capitalAmount = parseInt(e.target.value) || 0;
            this.sendFilterUpdate();
        });

        // 表头点击排序
        this.elements.fundingTable.addEventListener('click', (e) => {
            if (e.target.tagName === 'TH' && e.target.dataset.column) {
                const column = e.target.dataset.column;
                if (this.currentSort.column === column) {
                    this.currentSort.direction = this.currentSort.direction === 'asc' ? 'desc' : 'asc';
                } else {
                    this.currentSort.column = column;
                    this.currentSort.direction = 'desc';
                }
                this.elements.sortColumn.value = this.currentSort.column;
                this.elements.sortDirection.value = this.currentSort.direction;
                this.updateDisplay();
            }
        });
    }

    sendFilterUpdate() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            const message = {
                type: 'filter_update',
                enabledExchanges: Array.from(this.enabledExchanges),
                capitalAmount: this.capitalAmount
            };
            this.ws.send(JSON.stringify(message));
        }
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}`;
        
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.updateConnectionStatus('connected', '已连接');
        };

        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.data = data;
                this.updateDisplay();
                this.updateStatus(data.status);
                this.updateLastUpdated(data.lastUpdated);
            } catch (error) {
                console.error('Failed to parse WebSocket message:', error);
            }
        };

        this.ws.onclose = () => {
            this.updateConnectionStatus('disconnected', '连接断开');
            setTimeout(() => this.connectWebSocket(), 3000);
        };

        this.ws.onerror = () => {
            this.updateConnectionStatus('error', '连接错误');
        };

        this.updateConnectionStatus('connecting', '连接中...');
    }

    updateConnectionStatus(status, text) {
        const statusDot = this.elements.connectionStatus.querySelector('.status-dot');
        const statusText = this.elements.connectionStatus.querySelector('.status-text');
        
        statusDot.className = `status-dot ${status}`;
        statusText.textContent = text;
    }

    updateLastUpdated(timestamp) {
        if (timestamp) {
            const date = new Date(timestamp);
            this.elements.lastUpdated.textContent = `最后更新: ${date.toLocaleTimeString()}`;
        }
    }

    updateStatus(status) {
        let hasActiveStatus = false;
        let statusHtml = '';

        if (status.edgex) {
            if (status.edgex.connecting) {
                statusHtml += '<div class="status-item"><span>EdgeX</span><span class="status-value">连接中...</span></div>';
                hasActiveStatus = true;
            } else if (!status.edgex.connected) {
                statusHtml += '<div class="status-item"><span>EdgeX</span><span class="status-value">连接断开</span></div>';
                hasActiveStatus = true;
            } else if (status.edgex.error) {
                statusHtml += `<div class="status-item"><span>EdgeX</span><span class="status-value">${status.edgex.error}</span></div>`;
                hasActiveStatus = true;
            }
        }

        ['lighter', 'grvt', 'aster', 'backpack'].forEach(exchange => {
            if (status[exchange] && status[exchange].refreshing) {
                statusHtml += `<div class="status-item"><span>${exchange.toUpperCase()}</span><span class="status-value">刷新中...</span></div>`;
                hasActiveStatus = true;
            }
            if (status[exchange] && status[exchange].error) {
                statusHtml += `<div class="status-item"><span>${exchange.toUpperCase()}</span><span class="status-value">${status[exchange].error}</span></div>`;
                hasActiveStatus = true;
            }
        });

        this.elements.statusPanel.innerHTML = statusHtml;
        this.elements.statusPanel.className = hasActiveStatus ? 'status-panel show' : 'status-panel';
    }

    formatRate(rate) {
        if (rate === undefined || rate === null) return '--';
        const percentage = (rate * 100).toFixed(4);
        const value = parseFloat(percentage);
        const className = value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral';
        return `<span class="funding-rate ${className}">${percentage}%</span>`;
    }

    getVisibleColumns() {
        const columns = ['symbol'];
        
        ['lighter', 'binance', 'hyperliquid', 'edgex', 'grvt', 'aster', 'backpack'].forEach(exchange => {
            if (this.enabledExchanges.has(exchange)) {
                columns.push(`${exchange}Funding`);
            }
        });

        return columns;
    }

    updateTableHeaders() {
        const thead = this.elements.fundingTable.querySelector('thead tr');
        const visibleColumns = this.getVisibleColumns();
        
        // 显示/隐藏表头
        thead.querySelectorAll('th').forEach(th => {
            const column = th.dataset.column;
            if (column) {
                th.style.display = visibleColumns.includes(column) ? '' : 'none';
                
                // 更新排序指示器
                th.className = 'funding-column sortable';
                if (column === this.currentSort.column) {
                    th.className += ` sorted-${this.currentSort.direction}`;
                }
            }
        });
    }

    sortData(rows) {
        return [...rows].sort((a, b) => {
            const aVal = a[this.currentSort.column];
            const bVal = b[this.currentSort.column];
            
            // 处理undefined/null值
            if (aVal === undefined || aVal === null) return 1;
            if (bVal === undefined || bVal === null) return -1;
            
            let comparison = 0;
            if (typeof aVal === 'string') {
                comparison = aVal.localeCompare(bVal);
            } else {
                comparison = aVal - bVal;
            }
            
            return this.currentSort.direction === 'asc' ? comparison : -comparison;
        });
    }

    filterRows(rows) {
        return rows.filter(row => {
            // 至少要有两个启用的交易所有数据才显示
            let enabledExchangeCount = 0;
            
            ['lighter', 'binance', 'hyperliquid', 'edgex', 'grvt', 'aster', 'backpack'].forEach(exchange => {
                if (this.enabledExchanges.has(exchange) && row[`${exchange}Funding`] !== undefined) {
                    enabledExchangeCount++;
                }
            });
            
            return enabledExchangeCount >= 2;
        });
    }

    updateDisplay() {
        if (!this.data.rows || this.data.rows.length === 0) {
            this.elements.fundingTableBody.innerHTML = '<tr><td colspan="8" class="loading">暂无数据</td></tr>';
            return;
        }

        // 更新表头
        this.updateTableHeaders();
        
        // 筛选和排序数据  
        let processedRows = this.filterRows(this.data.rows);
        processedRows = this.sortData(processedRows);
        processedRows = processedRows.slice(0, this.displayLimit);

        // 更新表格内容
        const visibleColumns = this.getVisibleColumns();
        const tbody = this.elements.fundingTableBody;
        
        if (processedRows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="loading">没有符合条件的数据</td></tr>';
            return;
        }

        tbody.innerHTML = processedRows.map(row => {
            return `<tr>
                ${visibleColumns.map(column => {
                    if (column === 'symbol') {
                        return `<td>${row.symbol}</td>`;
                    } else {
                        const rate = row[column];
                        return `<td>${this.formatRate(rate)}</td>`;
                    }
                }).join('')}
            </tr>`;
        }).join('');

        // 更新价差
        this.updateSpreads();
    }

    updateSpreads() {
        if (!this.data.spreads || this.data.spreads.length === 0) {
            this.elements.spreadsGrid.innerHTML = '<div class="loading">暂无价差数据</div>';
            return;
        }

        // 服务器端已经根据筛选计算了价差，直接显示
        this.elements.spreadsGrid.innerHTML = this.data.spreads.map(spread => {
            return `
                <div class="spread-card">
                    <div class="symbol">${spread.symbol}</div>
                    <div class="diff">${(spread.diff * 100).toFixed(4)}%</div>
                    <div class="arbitrage-info">
                        <div class="exchange-action sell-action">
                            <div class="action-label">SELL (做空)</div>
                            <div class="exchange-name">${spread.high.exchange}</div>
                            <div class="exchange-rate positive">${(spread.high.rate * 100).toFixed(4)}%</div>
                            <div class="action-desc">收取资金费率</div>
                        </div>
                        <div class="arrow">⚡</div>
                        <div class="exchange-action buy-action">
                            <div class="action-label">BUY (做多)</div>
                            <div class="exchange-name">${spread.low.exchange}</div>
                            <div class="exchange-rate negative">${(spread.low.rate * 100).toFixed(4)}%</div>
                            <div class="action-desc">支付资金费率</div>
                        </div>
                    </div>
                    <div class="profit">
                        <div class="profit-24h">24h 预期收益: ${(spread.estimated24hProfit * 100).toFixed(2)}%</div>
                        ${spread.estimated24hProfitAmount ? 
                            `<div class="profit-amount">$${spread.estimated24hProfitAmount.toFixed(2)}</div>` : 
                            ''}
                    </div>
                </div>
            `;
        }).join('');
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', () => {
    new FundingMonitor();
});
