document.addEventListener('DOMContentLoaded', () => {
    initTabs();
    initChart();
    initDetailFilters(); // 필터 로직 실행
});

// --- 탭 기능 ---
function initTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    const contents = document.querySelectorAll('.tab-content');
    if (!tabs.length) return;

    tabs.forEach(btn => {
        btn.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.style.display = 'none'); // 즉시 숨김

            btn.classList.add('active');
            const targetId = 'tab-' + btn.dataset.tab;
            const target = document.getElementById(targetId);
            if (target) {
                target.style.display = 'block';
                setTimeout(() => target.classList.add('active'), 10);
            }
        });
    });
}

// --- 차트 기능 ---
// 차트 인스턴스를 저장할 변수
// 차트 인스턴스 저장 변수
let myPriceChart = null;

// 페이지 로드 시 호출되는 초기화 함수
function initChart() {
    // 기본값: 최근 7일 (전체 흐름)
    updateChartPeriod(7);
}

// 기간 변경 및 차트 그리기 메인 함수
function updateChartPeriod(days) {
    const ctx = document.getElementById('priceChart');
    const noData = document.getElementById('noChartData');
    
    // 캔버스가 없으면 중단
    if (!ctx) return;

    // 1. 데이터 가져오기
    const allData = window.ITEM_CHART_DATA || {};
    const timestamps = allData.timestamps || [];
    
    // 데이터가 없으면 '데이터 없음' 표시
    if (timestamps.length === 0) {
        ctx.style.display = 'none';
        if (noData) noData.style.display = 'block';
        return;
    }

    // 2. 시간 필터링 (최근 N일)
    // 마지막 데이터(최신) 시간을 기준으로 days * 24시간 전까지만 자름
    const lastTime = timestamps[timestamps.length - 1];
    const cutoffTime = lastTime - (days * 24 * 60 * 60);

    const filteredIndices = [];
    timestamps.forEach((ts, idx) => {
        if (ts >= cutoffTime) filteredIndices.push(idx);
    });

    // 배열 자르기 헬퍼 함수
    const sliceData = (arr) => filteredIndices.map(i => arr[i]);

    const slicedData = {
        dates: sliceData(allData.dates),
        soldAvg: sliceData(allData.soldAvg),
        soldMin: sliceData(allData.soldMin),
        soldMax: sliceData(allData.soldMax),
        regAvg: sliceData(allData.regAvg),
        regMin: sliceData(allData.regMin),
        regMax: sliceData(allData.regMax)
    };

    // 3. 보기 모드 설정 (1일이 아니면 상세 막대를 숨김)
    const hideDetails = (days !== 1);

    // 4. 버튼 활성화 UI 업데이트
    document.querySelectorAll('.btn-chart-period').forEach(btn => btn.classList.remove('active'));
    const activeBtn = document.getElementById(`btnPeriod${days}`);
    if (activeBtn) activeBtn.classList.add('active');

    // ============================================================
    // [헬퍼] 금액 포맷팅 함수 (10만 이상 '만' 단위 + G)
    // 예: 750000 -> "75만", 1000 -> "1,000"
    // ============================================================
    const formatMoney = (val) => {
        if (val === null || val === undefined) return '';
        if (val >= 100000) {
            // 1만 단위로 나누고 소수점 최대 1자리
            let unitVal = val / 10000;
            return unitVal.toLocaleString(undefined, { maximumFractionDigits: 1 }) + '만';
        }
        return val.toLocaleString();
    };

    // 5. 기존 차트 파괴 후 재생성
    if (myPriceChart) {
        myPriceChart.destroy();
    }

    myPriceChart = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: slicedData.dates,
            datasets: [
                // ============ [판매 데이터] ============
                {
                    type: 'line',
                    label: '판매 평균',
                    data: slicedData.soldAvg,
                    borderColor: '#ef4444',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 3,
                    pointStyle: 'circle', // 그래프 상의 점은 동그라미
                    order: 1,
                    tooltipOrder: 1 // [툴팁 순서 1등]
                },
                {
                    type: 'bar',
                    label: '판매 최저', // 순서상 등록평균 뒤에 오게 하려고 일단 정의
                    data: slicedData.soldMin,
                    backgroundColor: 'rgba(239, 68, 68, 0.25)',
                    borderColor: 'transparent',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8,
                    order: 2,
                    stack: 'sold',
                    hidden: hideDetails,
                    tooltipOrder: 3 // [툴팁 순서 3등]
                },
                {
                    type: 'bar',
                    label: '판매 최고',
                    data: slicedData.soldMax,
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    borderColor: 'transparent',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8,
                    order: 3,
                    stack: 'sold',
                    hidden: hideDetails,
                    tooltipOrder: 4 // [툴팁 순서 4등]
                },

                // ============ [등록 데이터] ============
                {
                    type: 'line',
                    label: '등록 평균',
                    data: slicedData.regAvg,
                    borderColor: '#3b82f6',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 3,
                    pointStyle: 'circle',
                    order: 1,
                    tooltipOrder: 2 // [툴팁 순서 2등]
                },
                {
                    type: 'bar',
                    label: '등록 최저',
                    data: slicedData.regMin,
                    backgroundColor: 'rgba(59, 130, 246, 0.25)',
                    borderColor: 'transparent',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8,
                    order: 2,
                    stack: 'reg',
                    hidden: hideDetails,
                    tooltipOrder: 5 // [툴팁 순서 5등]
                },
                {
                    type: 'bar',
                    label: '등록 최고',
                    data: slicedData.regMax,
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    borderColor: 'transparent',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8,
                    order: 3,
                    stack: 'reg',
                    hidden: hideDetails,
                    tooltipOrder: 6 // [툴팁 순서 6등]
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { 
                    stacked: true, 
                    grid: { display: false }, 
                    ticks: { maxTicksLimit: hideDetails ? 8 : 6, maxRotation: 0 } 
                },
                y: { 
                    stacked: false, 
                    beginAtZero: false, 
                    grid: { color: '#f1f5f9' }, 
                    ticks: { callback: v => formatMoney(v) } 
                }
            },
            plugins: {
                legend: { 
                    position: 'top', 
                    labels: { 
                        usePointStyle: true,
                        boxWidth: 20,
                        generateLabels: function(chart) {
                            const original = Chart.defaults.plugins.legend.labels.generateLabels(chart);
                            original.forEach(label => {
                                // 범례: 평균은 선, 나머지는 동그라미
                                if (label.text.includes('평균')) label.pointStyle = 'line';
                                else label.pointStyle = 'circle';
                            });
                            if (hideDetails) {
                                return original.filter(item => !item.text.includes('최고') && !item.text.includes('최저'));
                            }
                            return original;
                        }
                    } 
                },
                tooltip: {
                    // [1] 정렬: 판매평균 -> 등록평균 -> 판매Min/Max -> 등록Min/Max
                    itemSort: (a, b) => a.dataset.tooltipOrder - b.dataset.tooltipOrder,
                    
                    // [2] 툴팁 아이콘 설정
                    usePointStyle: true, // 필수 활성화
                    callbacks: {
                        labelColor: function(context) {
                            // 1. 평균 (판매 평균, 등록 평균) -> 선(Line)
                            if (context.dataset.label.includes('평균')) {
                                return {
                                    borderColor: context.dataset.borderColor, // 선 색상
                                    borderWidth: 3, // 선 두께
                                    pointStyle: 'line',
                                    rotation: 0,
                                    borderRadius: 0
                                };
                            } 
                            // 2. 나머지 (최저, 최고) -> 네모(Rect)
                            else {
                                return {
                                    borderColor: context.dataset.backgroundColor, // 테두리도 배경색과 같게
                                    backgroundColor: context.dataset.backgroundColor, // 면 색상
                                    borderWidth: 0,
                                    pointStyle: 'rect', // [핵심] 네모 모양 강제
                                    rotation: 0,
                                    borderRadius: 0
                                };
                            }
                        },
                        
                        // [3] 텍스트 내용 (가격 + 수량)
                        label: function(context) {
                            let label = context.dataset.label || '';
                            let val = context.parsed.y;
                            if (val === null) return '';
                            
                            let res = `${label}: ${formatMoney(val)} 골드`;
                            
                            if (label.includes('평균')) {
                                const originalIdx = filteredIndices[context.dataIndex];
                                let count = 0;
                                if (label.includes('판매')) {
                                    count = window.ITEM_CHART_DATA.soldCount[originalIdx] || 0;
                                } else if (label.includes('등록')) {
                                    count = window.ITEM_CHART_DATA.regCount[originalIdx] || 0;
                                }
                                res += ` (${count}개)`;
                            }
                            return res;
                        }
                    }
                }
            }
        }
    });
}

// --- [핵심] 상세 검색 필터 로직 (동적 추가 방식) ---
function initDetailFilters() {
    const select = document.getElementById('detailFilterSelect');
    const container = document.getElementById('detailActiveFilters');
    const resetBtn = document.getElementById('resetFilters');

    if (!select || !container) return;

    // [요청하신 상세 옵션 목록 적용]
    const FILTER_CONFIG = [
        { id: 'balance', label: '밸런스', type: 'range', names: ['min_balance', 'max_balance'] },
        { id: 'durability', label: '내구도', type: 'range', names: ['min_dur', 'max_dur'] }, // 내구도 추가
        { id: 'reforge', label: '세공 옵션', type: 'triple_text', names: ['reforge_1', 'reforge_2', 'reforge_3'] },
        { id: 'color', label: '색상 (RGB)', type: 'color_range', names: ['r_min', 'r_max', 'g_min', 'g_max', 'b_min', 'b_max'] },
        { id: 'erg', label: '에르그 종류', type: 'text', names: ['erg_type'], placeholder: '종류 (예: 카타르)' },
        { id: 'echo_awake', label: '에코스톤 각성', type: 'text', names: ['echo_awake_name'], placeholder: '능력명 입력' },
        { id: 'echo_grade', label: '에코스톤 등급', type: 'range', names: ['min_echo_grade', 'max_echo_grade'] },
        { id: 'echo_unique', label: '에코스톤 고유', type: 'range', names: ['min_echo_unique', 'max_echo_unique'] },
        { id: 'set_effect', label: '세트 효과', type: 'text', names: ['set_effect_name'], placeholder: '효과명 입력' },
        { id: 'enchant', label: '인챈트 (접두/접미)', type: 'double_text', names: ['enchant_prefix', 'enchant_suffix'], labels: ['접두', '접미'] },
        { id: 'enchant_type', label: '인챈트 종류', type: 'double_text', names: ['enchant_type', 'enchant_name'], labels: ['종류', '명칭'] },
        { id: 'totem_option', label: '토템 추가 옵션', type: 'text', names: ['totem_option'], placeholder: '옵션 입력' },
        { id: 'totem_effect', label: '토템 효과', type: 'text', names: ['totem_effect'], placeholder: '효과 입력' },
        { id: 'special_up', label: '특별 개조', type: 'text', names: ['special_upgrade'], placeholder: '종류 (R/S)' },
        { id: 'pet_race', label: '펫 종족명', type: 'text', names: ['pet_race'], placeholder: '종족명 입력' },
        { id: 'display_max', label: '표기맥 (맥뎀)', type: 'range', names: ['min_max_att', 'max_max_att'] },
    ];

    // 1. Select Box에 옵션 채우기
    FILTER_CONFIG.forEach(cfg => {
        const opt = document.createElement('option');
        opt.value = cfg.id;
        opt.textContent = cfg.label;
        select.appendChild(opt);
    });

    // 2. 선택 시 필터 행 추가
    select.addEventListener('change', () => {
        const selectedId = select.value;
        if (!selectedId) return;

        if (document.getElementById(`filter-row-${selectedId}`)) {
            alert('이미 추가된 조건입니다.');
            select.value = "";
            return;
        }

        const config = FILTER_CONFIG.find(c => c.id === selectedId);
        createFilterRow(config, container);
        select.value = ""; // 선택 초기화
    });

    // 3. 초기화 버튼
    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            container.innerHTML = '';
        });
    }
}

function createFilterRow(config, container) {
    const row = document.createElement('div');
    row.className = 'filter-row';
    row.id = `filter-row-${config.id}`;

    // --- 헤더 (라벨 + 삭제버튼) ---
    // 사이드바 디자인에 맞춰, 헤더 안에 삭제 버튼을 넣습니다.
    const header = document.createElement('div');
    header.className = 'fr-header';
    
    // 라벨
    const labelSpan = document.createElement('span');
    labelSpan.innerHTML = config.label;
    header.appendChild(labelSpan);

    // 삭제 버튼
    const removeBtn = document.createElement('button');
    removeBtn.className = 'fr-remove';
    removeBtn.type = 'button';
    removeBtn.innerHTML = '<i class="ph ph-x"></i>';
    removeBtn.onclick = () => row.remove();
    header.appendChild(removeBtn);

    row.appendChild(header);

    // --- 바디 (입력 폼) ---
    const body = document.createElement('div');
    body.className = 'fr-body';

    // 1. 범위 입력 (Min ~ Max)
    if (config.type === 'range') {
        body.innerHTML = `
            <div class="range-group">
                <input type="number" name="${config.names[0]}" class="fr-input short" placeholder="Min">
                <span class="fr-dash">~</span>
                <input type="number" name="${config.names[1]}" class="fr-input short" placeholder="Max">
            </div>
        `;
    } 
    // 2. 단일 텍스트
    else if (config.type === 'text') {
        body.innerHTML = `
            <input type="text" name="${config.names[0]}" class="fr-input" placeholder="${config.placeholder || ''}">
        `;
    } 
    // 3. 더블 텍스트 (인챈트 등) - 사이드바 폭 고려하여 세로 배치 혹은 꽉 찬 가로 배치
    else if (config.type === 'double_text') {
        const label1 = config.labels ? config.labels[0] : '값1';
        const label2 = config.labels ? config.labels[1] : '값2';
        
        // [수정] flex-direction: column 추가하여 세로로 배치
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:6px; width:100%;">
                <input type="text" name="${config.names[0]}" class="fr-input" placeholder="${label1}">
                <input type="text" name="${config.names[1]}" class="fr-input" placeholder="${label2}">
            </div>
        `;
    }
    // 4. 트리플 텍스트 (세공 등) - 3개를 한 줄에 넣기 좁으므로 세로로 쌓음
    else if (config.type === 'triple_text') {
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:5px; width:100%;">
                <input type="text" name="${config.names[0]}" class="fr-input" placeholder="옵션 1순위">
                <input type="text" name="${config.names[1]}" class="fr-input" placeholder="옵션 2순위">
                <input type="text" name="${config.names[2]}" class="fr-input" placeholder="옵션 3순위">
            </div>
        `;
    } 
    // 5. RGB 색상 범위 (6개 입력값)
    else if (config.type === 'color_range') {
        // R, G, B 각각 한 줄씩 차지하도록 구성
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:6px; width:100%;">
                <div class="range-group">
                    <span style="color:#ef4444; font-weight:bold; width:15px;">R</span>
                    <input type="number" name="${config.names[0]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[1]}" class="fr-input short" placeholder="255">
                </div>
                <div class="range-group">
                    <span style="color:#10b981; font-weight:bold; width:15px;">G</span>
                    <input type="number" name="${config.names[2]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[3]}" class="fr-input short" placeholder="255">
                </div>
                <div class="range-group">
                    <span style="color:#3b82f6; font-weight:bold; width:15px;">B</span>
                    <input type="number" name="${config.names[4]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[5]}" class="fr-input short" placeholder="255">
                </div>
            </div>
        `;
    }

    row.appendChild(body);
    container.appendChild(row);
}

function createFilterRow(config, container) {
    const row = document.createElement('div');
    row.className = 'filter-row';
    row.id = `filter-row-${config.id}`;

    // 1. 헤더 (라벨 + 삭제버튼)
    const header = document.createElement('div');
    header.className = 'fr-header';
    
    // 라벨
    const labelSpan = document.createElement('span');
    labelSpan.textContent = config.label;
    header.appendChild(labelSpan);

    // 삭제 버튼
    const removeBtn = document.createElement('button');
    removeBtn.className = 'fr-remove';
    removeBtn.type = 'button';
    removeBtn.innerHTML = '<i class="ph ph-x"></i>';
    removeBtn.onclick = () => row.remove();
    header.appendChild(removeBtn);

    row.appendChild(header);

    // 2. 바디 (입력 폼 생성)
    const body = document.createElement('div');
    body.className = 'fr-body';

    // [Type 1] 범위 입력 (Min ~ Max)
    if (config.type === 'range') {
        body.innerHTML = `
            <div class="range-group">
                <input type="number" name="${config.names[0]}" class="fr-input short" placeholder="Min">
                <span class="fr-dash">~</span>
                <input type="number" name="${config.names[1]}" class="fr-input short" placeholder="Max">
            </div>
        `;
    } 
    // [Type 2] 단일 텍스트
    else if (config.type === 'text') {
        body.innerHTML = `
            <input type="text" name="${config.names[0]}" class="fr-input" placeholder="${config.placeholder || ''}">
        `;
    }
    // [Type 3] 셀렉트 박스
    else if (config.type === 'select') {
        const opts = config.options.map(o => `<option value="${o}">${o}</option>`).join('');
        body.innerHTML = `<select class="filter-select" style="width:100%;">${opts}</select>`;
    } 
    // [Type 4] 2개 입력 (세로 배치 수정됨)
    else if (config.type === 'double_text') {
        const label1 = config.labels ? config.labels[0] : '값1';
        const label2 = config.labels ? config.labels[1] : '값2';
        
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:6px; width:100%;">
                <input type="text" name="${config.names[0]}" class="fr-input" placeholder="${label1}">
                <input type="text" name="${config.names[1]}" class="fr-input" placeholder="${label2}">
            </div>
        `;
    } 
    // [Type 5] 3개 입력 (세공 옵션)
    else if (config.type === 'triple_text') {
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:6px; width:100%;">
                <input type="text" name="${config.names[0]}" class="fr-input" placeholder="1순위 옵션">
                <input type="text" name="${config.names[1]}" class="fr-input" placeholder="2순위 옵션">
                <input type="text" name="${config.names[2]}" class="fr-input" placeholder="3순위 옵션">
            </div>
        `;
    } 
    // [Type 6] 색상 RGB
    else if (config.type === 'color_range') {
        body.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:6px; width:100%;">
                <div class="range-group">
                    <span style="color:#ef4444; font-weight:800; width:15px;">R</span>
                    <input type="number" name="${config.names[0]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[1]}" class="fr-input short" placeholder="255">
                </div>
                <div class="range-group">
                    <span style="color:#10b981; font-weight:800; width:15px;">G</span>
                    <input type="number" name="${config.names[2]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[3]}" class="fr-input short" placeholder="255">
                </div>
                <div class="range-group">
                    <span style="color:#3b82f6; font-weight:800; width:15px;">B</span>
                    <input type="number" name="${config.names[4]}" class="fr-input short" placeholder="0">
                    <span class="fr-dash">~</span>
                    <input type="number" name="${config.names[5]}" class="fr-input short" placeholder="255">
                </div>
            </div>
        `;
    }

    row.appendChild(body);
    container.appendChild(row);
}