
const BASE_URL = 'http://localhost:8080/ingestion';

async function updateUI() {
    console.log('Updating UI');
    const source = document.getElementById('source').value;
    document.getElementById('clickhouse-config').style.display = source === 'clickhouse' ? 'block' : 'none';
    document.getElementById('flatfile-config').style.display = source === 'flatfile' ? 'block' : 'none';
    document.getElementById('join-config').style.display = source === 'clickhouse' ? 'block' : 'none';
    document.getElementById('table-name').innerHTML = '';
    document.getElementById('columns-list').innerHTML = '';
    document.getElementById('preview-section').style.display = 'none';
    document.getElementById('status').textContent = 'Ready';
    document.getElementById('result').textContent = '';
    document.getElementById('progress').style.width = '0%';
    document.getElementById('output-label').textContent = source === 'flatfile' ? 'Target Table Name:' : 'Output File Path:';
}

async function connect() {
    console.log('Connecting to ClickHouse');
    document.getElementById('status').textContent = 'Connecting...';
    try {
        const response = await fetch(`${BASE_URL}/tables`);
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        document.getElementById('status').textContent = 'Connected';
        showResult('Successfully connected to ClickHouse');
    } catch (e) {
        console.error('Connection error:', e);
        document.getElementById('status').textContent = 'Error';
        showResult('Connection failed: ' + e.message, true);
    }
}

async function fetchTables() {
    console.log('Fetching tables');
    const source = document.getElementById('source').value;
    if (source !== 'clickhouse') {
        document.getElementById('status').textContent = 'Select ClickHouse source';
        return;
    }
    try {
        document.getElementById('status').textContent = 'Fetching tables...';
        const response = await fetch(`${BASE_URL}/tables`);
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        const tables = await response.json();
        const select = document.getElementById('table-name');
        select.innerHTML = '<option value="">Select a table</option>' + 
                          tables.map(t => `<option value="${t}">${t}</option>`).join('');
        document.getElementById('status').textContent = 'Tables loaded';
        showResult('Tables loaded successfully');
    } catch (e) {
        console.error('Fetch tables error:', e);
        document.getElementById('status').textContent = 'Error';
        showResult('Error fetching tables: ' + e.message, true);
    }
}

async function fetchColumns() {
    console.log('Fetching columns');
    const source = document.getElementById('source').value;
    const tableName = document.getElementById('table-name').value;
    const fileUpload = document.getElementById('file-upload').files[0];
    let url = `${BASE_URL}/columns?source=${source}`;
    
    document.getElementById('status').textContent = 'Fetching columns...';
    try {
        if (source === 'clickhouse') {
            if (!tableName) {
                document.getElementById('status').textContent = 'Error';
                showResult('Please select a table', true);
                return;
            }
            url += `&tableName=${tableName}`;
        } else if (source === 'flatfile') {
            if (!fileUpload) {
                document.getElementById('status').textContent = 'Error';
                showResult('Please upload a CSV file', true);
                return;
            }
            console.log('Uploading CSV:', fileUpload.name);
            const formData = new FormData();
            formData.append('file', fileUpload);
            document.getElementById('status').textContent = 'Uploading file...';
            const uploadResponse = await fetch(`${BASE_URL}/upload`, {
                method: 'POST',
                body: formData
            });
            if (!uploadResponse.ok) {
                const errorText = await uploadResponse.text();
                throw new Error(`File upload failed: ${errorText}`);
            }
            const filePath = await uploadResponse.text();
            console.log('File uploaded to:', filePath);
            url += `&filePath=${encodeURIComponent(filePath)}`;
        } else {
            throw new Error('Invalid source');
        }

        console.log('Fetching columns from:', url);
        const response = await fetch(url);
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP ${response.status}: ${errorText}`);
        }
        const columns = await response.json();
        console.log('Columns received:', columns);
        if (source === 'flatfile' && columns.some(col => /\s/.test(col))) {
            console.warn('Warning: CSV headers contain spaces, which will be handled by the backend.');
            showResult('Note: Headers with spaces detected. They will be sanitized for ClickHouse.', false);
        }
        const list = document.getElementById('columns-list');
        list.innerHTML = columns.map(c => `
            <label><input type="checkbox" value="${c}" checked> ${c}</label><br>
        `).join('');
        document.getElementById('status').textContent = 'Columns loaded';
        showResult('Columns loaded successfully');
    } catch (e) {
        console.error('Fetch columns error:', e);
        document.getElementById('status').textContent = 'Error';
        let errorMessage = e.message;
        if (e.message.includes('Failed to fetch')) {
            errorMessage = 'Failed to connect to backend. Check if backend is running and CORS is configured.';
        }
        showResult('Error fetching columns: ' + errorMessage, true);
    }
}

function addJoinCondition() {
    console.log('Adding join condition');
    const container = document.getElementById('join-conditions');
    const div = document.createElement('div');
    div.innerHTML = `
        <input type="text" class="join-table" placeholder="Join Table">
        <input type="text" class="join-key" placeholder="Join Key">
        <button onclick="this.parentElement.remove()">Remove</button>
    `;
    container.appendChild(div);
}

async function previewData() {
    console.log('Previewing data');
    document.getElementById('status').textContent = 'Fetching preview...';
    const source = document.getElementById('source').value;
    const tableName = document.getElementById('table-name').value;
    const outputPath = document.getElementById('output-path').value;
    const jwtToken = document.getElementById('ch-jwt').value;
    const columns = Array.from(document.querySelectorAll('#columns-list input:checked'))
                        .map(input => input.value);
    let filePath;

    if (!columns.length) {
        document.getElementById('status').textContent = 'Error';
        showResult('Please select at least one column', true);
        return;
    }

    if (source === 'flatfile') {
        const fileUpload = document.getElementById('file-upload').files[0];
        if (!fileUpload) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please upload a CSV file', true);
            return;
        }
        console.log('Uploading file for preview:', fileUpload.name);
        const formData = new FormData();
        formData.append('file', fileUpload);
        try {
            const uploadResponse = await fetch(`${BASE_URL}/upload`, {
                method: 'POST',
                body: formData
            });
            if (!uploadResponse.ok) throw new Error(`File upload failed: ${await uploadResponse.text()}`);
            filePath = await uploadResponse.text();
        } catch (e) {
            console.error('Upload error:', e);
            document.getElementById('status').textContent = 'Error';
            showResult('File upload failed: ' + e.message, true);
            return;
        }
    } else {
        if (!tableName) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please select a table', true);
            return;
        }
        filePath = outputPath;
    }

    const joinConditions = Array.from(document.querySelectorAll('#join-conditions > div')).map(div => ({
        table: div.querySelector('.join-table').value,
        key: div.querySelector('.join-key').value
    })).filter(c => c.table && c.key);

    const request = {
        source,
        tableName,
        columns,
        filePath,
        jwtToken,
        joinConditions: source === 'clickhouse' ? joinConditions : []
    };

    try {
        const response = await fetch(`${BASE_URL}/preview`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        const rows = await response.json();
        displayPreview(rows, columns);
        document.getElementById('status').textContent = 'Preview loaded';
        showResult('Preview loaded successfully');
    } catch (e) {
        console.error('Preview error:', e);
        document.getElementById('status').textContent = 'Error';
        showResult('Preview failed: ' + e.message, true);
    }
}

function displayPreview(rows, columns) {
    console.log('Displaying preview:', rows);
    const headers = document.getElementById('preview-headers');
    const body = document.getElementById('preview-body');
    headers.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
    body.innerHTML = rows.map(row => `
        <tr>${columns.map(c => `<td>${row[c] || ''}</td>`).join('')}</tr>
    `).join('');
    document.getElementById('preview-section').style.display = 'block';
}

async function startIngestion() {
    console.log('Starting ingestion');
    document.getElementById('status').textContent = 'Preparing ingestion...';
    document.getElementById('progress').style.width = '0%';
    const source = document.getElementById('source').value;
    const tableName = source === 'clickhouse' ? document.getElementById('table-name').value : document.getElementById('output-path').value;
    const outputPath = document.getElementById('output-path').value;
    const jwtToken = document.getElementById('ch-jwt').value;
    const columns = Array.from(document.querySelectorAll('#columns-list input:checked'))
                        .map(input => input.value);
    let filePath;

    if (!columns.length) {
        document.getElementById('status').textContent = 'Error';
        showResult('Please select at least one column', true);
        return;
    }

    if (source === 'flatfile') {
        const fileUpload = document.getElementById('file-upload').files[0];
        if (!fileUpload) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please upload a CSV file', true);
            return;
        }
        if (!tableName) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please enter a target table name', true);
            return;
        }
        console.log('Uploading file:', fileUpload.name);
        const formData = new FormData();
        formData.append('file', fileUpload);
        try {
            document.getElementById('status').textContent = 'Uploading file...';
            document.getElementById('progress').style.width = '20%';
            const uploadResponse = await fetch(`${BASE_URL}/upload`, {
                method: 'POST',
                body: formData
            });
            if (!uploadResponse.ok) throw new Error(`File upload failed: ${await uploadResponse.text()}`);
            filePath = await uploadResponse.text();
            console.log('File uploaded, path:', filePath);
        } catch (e) {
            console.error('Upload error:', e);
            document.getElementById('status').textContent = 'Error';
            showResult('File upload failed: ' + e.message, true);
            return;
        }
    } else {
        if (!tableName) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please select a source table', true);
            return;
        }
        if (!outputPath) {
            document.getElementById('status').textContent = 'Error';
            showResult('Please enter an output file path', true);
            return;
        }
        filePath = outputPath;
    }

    const joinConditions = Array.from(document.querySelectorAll('#join-conditions > div')).map(div => ({
        table: div.querySelector('.join-table').value,
        key: div.querySelector('.join-key').value
    })).filter(c => c.table && c.key);

    const request = {
        source,
        tableName,
        columns,
        filePath,
        jwtToken,
        joinConditions: source === 'clickhouse' ? joinConditions : []
    };

    console.log('Sending ingestion request:', request);
    document.getElementById('status').textContent = 'Ingesting...';
    document.getElementById('progress').style.width = '50%';

    try {
        const response = await fetch(`${BASE_URL}/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Server error: ${errorText}`);
        }
        const result = await response.text();
        console.log('Ingestion result:', result);
        document.getElementById('status').textContent = 'Completed';
        document.getElementById('progress').style.width = '100%';
        showResult(source === 'clickhouse' ? `CSV created successfully: ${result}` : result);
    } catch (e) {
        console.error('Ingestion error:', e);
        document.getElementById('status').textContent = 'Error';
        let errorMessage = e.message;
        if (e.message.includes('DB::Exception')) {
            errorMessage = 'ClickHouse error - check table columns or join conditions for validity';
        }
        showResult('Ingestion failed: ' + errorMessage, true);
    }
}

function showResult(message, isError = false) {
    console.log('Showing result:', message);
    const resultDiv = document.getElementById('result');
    resultDiv.textContent = message;
    resultDiv.className = isError ? 'error' : '';
}
async function addJoinCondition() {
    const container = document.getElementById('join-conditions');
    const div = document.createElement('div');
    div.className = 'join-condition';
    div.innerHTML = `
        <select class="join-type" onchange="updateJoinPreview()">
            <option value="INNER">INNER JOIN</option>
            <option value="LEFT">LEFT JOIN</option>
            <option value="RIGHT">RIGHT JOIN</option>
        </select>
        <span class="main-table-preview"></span>
        <select class="join-table-select" onchange="loadJoinColumns(this)"></select>
        <select class="join-column-select"></select>
        <button onclick="this.parentElement.remove()">Remove</button>
    `;
    container.appendChild(div);
    await populateJoinTables(div.querySelector('.join-table-select'));
}

async function populateJoinTables(selectElement) {
    try {
        const response = await fetch(`${BASE_URL}/tables`);
        if (!response.ok) throw new Error('Failed to fetch tables');
        const tables = await response.json();
        
        selectElement.innerHTML = `
            <option value="">Select Join Table</option>
            ${tables.map(t => `<option value="${t}">${t}</option>`).join('')}
        `;
    } catch (error) {
        console.error('Error loading tables:', error);
        showResult(`Error loading tables: ${error.message}`, true);
    }
}

async function loadJoinColumns(selectElement) {
    const table = selectElement.value;
    const row = selectElement.closest('.join-condition');
    const columnSelect = row.querySelector('.join-column-select');
    
    try {
        const response = await fetch(
            `${BASE_URL}/columns?source=clickhouse&tableName=${encodeURIComponent(table)}`
        );
        if (!response.ok) throw new Error('Failed to fetch columns');
        const columns = await response.json();
        
        columnSelect.innerHTML = columns.map(c => 
            `<option value="${c}">${c}</option>`
        ).join('');
    } catch (error) {
        console.error('Error loading columns:', error);
        showResult(`Error loading columns: ${error.message}`, true);
    }
}

function updateJoinPreview() {
    document.querySelectorAll('.join-condition').forEach(row => {
        const mainTable = document.getElementById('table-name').value;
        const joinType = row.querySelector('.join-type').value;
        const joinTable = row.querySelector('.join-table-select').value;
        row.querySelector('.main-table-preview').textContent = 
            `${mainTable} ${joinType} JOIN ${joinTable} ON `;
    });
}
async function populateJoinTables(selectElement) {
    try {
        // Clear existing options first
        selectElement.innerHTML = '<option value="">Loading tables...</option>';
        
        const response = await fetch(`${BASE_URL}/tables`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const tables = await response.json();
        
        selectElement.innerHTML = '<option value="">Select Table</option>' +
            tables.map(t => `<option value="${t}">${t}</option>`).join('');
            
    } catch (error) {
        console.error('Table load failed:', error);
        selectElement.innerHTML = '<option value="">Error loading tables</option>';
    }
}

async function loadJoinColumns(selectElement) {
    const table = selectElement.value;
    const columnSelect = selectElement.closest('.join-condition').querySelector('.join-column-select');
    
    if (!table) {
        columnSelect.innerHTML = '<option value="">Select table first</option>';
        return;
    }

    try {
        columnSelect.innerHTML = '<option value="">Loading columns...</option>';
        const response = await fetch(
            `${BASE_URL}/columns?source=clickhouse&tableName=${encodeURIComponent(table)}`
        );
        
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const columns = await response.json();
        
        columnSelect.innerHTML = columns.length > 0 
            ? columns.map(c => `<option value="${c}">${c}</option>`).join('')
            : '<option value="">No columns found</option>';
            
    } catch (error) {
        console.error('Column load failed:', error);
        columnSelect.innerHTML = '<option value="">Error loading columns</option>';
    }
}