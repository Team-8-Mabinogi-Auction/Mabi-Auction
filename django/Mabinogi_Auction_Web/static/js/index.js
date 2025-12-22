/* static/js/index.js */

/* ==========================================================================
   Initialization (DOM Ready)
   ========================================================================== */
document.addEventListener('DOMContentLoaded', () => {
    
    // 1. 상세 검색(동적 필터) 시스템 초기화
    initDynamicFilters();
    
    // 2. 서버에서 렌더링된 상태 업데이트 (총 개수 등)
    // Django 템플릿에서 이미 렌더링했으므로 JS가 덮어쓰지 않도록 합니다.
});


/* ==========================================================================
   Dynamic Filter System Logic (기존 코드 유지)
   ========================================================================== */
function initDynamicFilters() {
    const FILTER_CONFIG = [
        { id: 'balance', label: '밸런스', type: 'range', names: ['min_balance', 'max_balance'] },
        { id: 'reforge', label: '세공 옵션', type: 'triple_text', names: ['reforge_1', 'reforge_2', 'reforge_3'] },
        { id: 'color', label: '색상 (RGB)', type: 'color_range', names: ['r_min', 'r_max', 'g_min', 'g_max', 'b_min', 'b_max'] },
        { id: 'erg', label: '에르그 종류', type: 'text', names: ['erg_type'], placeholder: '종류 입력 (예: 카타르)' },
        { id: 'echo_awake', label: '에코스톤 각성 능력', type: 'text', names: ['echo_awake_name'], placeholder: '능력명 입력' },
        { id: 'echo_grade', label: '에코스톤 등급', type: 'range', names: ['min_echo_grade', 'max_echo_grade'] },
        { id: 'echo_unique', label: '에코스톤 고유 능력', type: 'range', names: ['min_echo_unique', 'max_echo_unique'] },
        { id: 'set_effect', label: '세트 효과', type: 'text', names: ['set_effect_name'], placeholder: '효과명 입력' },
        { id: 'enchant', label: '인챈트 (접두/접미)', type: 'double_text', names: ['enchant_prefix', 'enchant_suffix'], labels: ['접두', '접미'] },
        { id: 'enchant_type', label: '인챈트 종류', type: 'double_text', names: ['enchant_type', 'enchant_name'], labels: ['종류', '명칭'] },
        { id: 'totem_option', label: '토템 추가 옵션', type: 'text', names: ['totem_option'], placeholder: '옵션 입력' },
        { id: 'totem_effect', label: '토템 효과', type: 'text', names: ['totem_effect'], placeholder: '효과 입력' },
        { id: 'special_up', label: '특별 개조', type: 'text', names: ['special_upgrade'], placeholder: '종류 (R/S)' },
        { id: 'pet_race', label: '펫 종족명', type: 'text', names: ['pet_race'], placeholder: '종족명 입력' },
        { id: 'display_max', label: '표기맥 (맥뎀)', type: 'range', names: ['min_max_att', 'max_max_att'] },
    ];

    const toggleBtn = document.getElementById('toggleFilterBtn');
    const controllerDiv = document.getElementById('filterController');
    const filterSelect = document.getElementById('filterSelect');
    const activeFilters = document.getElementById('activeFilters');

    if (!toggleBtn || !controllerDiv || !filterSelect || !activeFilters) return;

    FILTER_CONFIG.forEach(config => {
        const option = document.createElement('option');
        option.value = config.id;
        option.textContent = config.label;
        filterSelect.appendChild(option);
    });

    toggleBtn.addEventListener('click', () => {
        const isHidden = controllerDiv.style.display === 'none';
        controllerDiv.style.display = isHidden ? 'block' : 'none';
        
        if (isHidden) {
            toggleBtn.classList.add('active');
            toggleBtn.innerHTML = '<i class="ph ph-caret-up"></i> 옵션 닫기';
        } else {
            toggleBtn.classList.remove('active');
            toggleBtn.innerHTML = '<i class="ph ph-faders"></i> 상세 검색 옵션';
        }
    });

    filterSelect.addEventListener('change', () => {
        const selectedId = filterSelect.value;
        if (!selectedId) return;

        if (document.getElementById(`filter-row-${selectedId}`)) {
            alert('이미 추가된 옵션입니다.');
            filterSelect.value = ""; 
            return;
        }

        const config = FILTER_CONFIG.find(c => c.id === selectedId);
        createFilterRow(config, activeFilters);
        
        filterSelect.value = "";
    });
}

function createFilterRow(config, container) {
    const row = document.createElement('div');
    row.className = 'filter-row';
    row.id = `filter-row-${config.id}`;

    const header = document.createElement('div');
    header.className = 'fr-header';
    header.innerHTML = `<i class="ph ph-check-square"></i> ${config.label}`;
    row.appendChild(header);

    const body = document.createElement('div');
    body.className = 'fr-body';

    if (config.type === 'range') {
        body.innerHTML = `
            <input type="number" name="${config.names[0]}" class="fr-input short" placeholder="Min">
            <span class="fr-dash">~</span>
            <input type="number" name="${config.names[1]}" class="fr-input short" placeholder="Max">
        `;
    } else if (config.type === 'text') {
        body.innerHTML = `
            <input type="text" name="${config.names[0]}" class="fr-input long" placeholder="${config.placeholder}">
        `;
    } else if (config.type === 'double_text') {
        body.innerHTML = `
            <span class="fr-label">${config.labels[0]}</span>
            <input type="text" name="${config.names[0]}" class="fr-input" style="width:100px;">
            <span class="fr-label" style="margin-left:8px;">${config.labels[1]}</span>
            <input type="text" name="${config.names[1]}" class="fr-input" style="width:100px;">
        `;
    } else if (config.type === 'triple_text') {
        body.innerHTML = `
            <input type="text" name="${config.names[0]}" class="fr-input" placeholder="1순위" style="width:30%">
            <input type="text" name="${config.names[1]}" class="fr-input" placeholder="2순위" style="width:30%">
            <input type="text" name="${config.names[2]}" class="fr-input" placeholder="3순위" style="width:30%">
        `;
    } else if (config.type === 'color_range') {
        body.innerHTML = `
            <span class="rgb-label" style="color:#ef4444;">R</span>
            <input type="number" name="${config.names[0]}" class="fr-input rgb" placeholder="0">
            <span class="fr-dash">~</span>
            <input type="number" name="${config.names[1]}" class="fr-input rgb" placeholder="255">
            
            <span class="rgb-label" style="color:#10b981;">G</span>
            <input type="number" name="${config.names[2]}" class="fr-input rgb" placeholder="0">
            <span class="fr-dash">~</span>
            <input type="number" name="${config.names[3]}" class="fr-input rgb" placeholder="255">
            
            <span class="rgb-label" style="color:#3b82f6;">B</span>
            <input type="number" name="${config.names[4]}" class="fr-input rgb" placeholder="0">
            <span class="fr-dash">~</span>
            <input type="number" name="${config.names[5]}" class="fr-input rgb" placeholder="255">
        `;
    }

    row.appendChild(body);

    const removeBtn = document.createElement('button');
    removeBtn.className = 'fr-remove';
    removeBtn.type = 'button';
    removeBtn.innerHTML = '<i class="ph ph-x"></i>';
    removeBtn.onclick = () => row.remove();
    
    row.appendChild(removeBtn);
    container.appendChild(row);
}

function serverSort(column) {
    const urlParams = new URLSearchParams(window.location.search);
    let currentSort = urlParams.get('sort');
    let currentDir = urlParams.get('dir');

    let newDir = 'desc';
    // 같은 컬럼을 또 클릭하면 방향 전환, 새로운 컬럼이면 asc로 시작
    if (currentSort === column) {
        newDir = (currentDir === 'desc') ? 'asc' : 'desc';
    }

    urlParams.set('sort', column);
    urlParams.set('dir', newDir);
    urlParams.set('page', 1); // 정렬 시 1페이지로 이동

    window.location.href = "?" + urlParams.toString();
}

// 페이지 로드 시 실행
window.onload = function() {
    // 브라우저의 탐색 유형을 확인
    const navEntries = performance.getEntriesByType('navigation');
    if (navEntries.length > 0 && navEntries[0].type === 'reload') {
        // 현재 URL에 파라미터(?sort= 등)가 있는지 확인
        if (window.location.search !== '') {
            console.log("새로고침 감지: 초기 화면으로 이동합니다.");
            window.location.href = window.location.pathname; // 파라미터 없이 주소만 호출 (예: /)
        }
    }
};