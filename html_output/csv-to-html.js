document.addEventListener('DOMContentLoaded', () => {
    document.querySelector('input[name="view-option"][value="by-posts"]').checked = true;
});

document.querySelector('#csvFileInput').addEventListener('change', parseCSV);
document.querySelectorAll('input[name="view-option"]').forEach(radio => {
    radio.addEventListener('click', handleRadioButtonClick);
});

let parsedData = [];
let htmlByUser = '';
let htmlByPost = '';

function parseCSV(event) {
    const file = event.target.files[0];
    const dataContainer = document.querySelector('#data-container');
    if (file) {
        htmlByUser = '';
        htmlByPost = '';
        dataContainer.innerHTML = '<p>Loading...</p>';
        const reader = new FileReader();
        reader.onload = function(evt) {
            const data = evt.target.result;
            const rows = escapeSpecialChars(data).filter(row => row.length > 0);
            parsedData = rows;
            displayDefaultView();
        };
        reader.readAsText(file);
        const viewOptions = document.getElementById('view-options');
        viewOptions.style.display="block";
    };
};


function escapeSpecialChars(data) {
    const rows = [];
    let row = [];
    let inQuotes = false;
    let value = '';

    for (let i = 0; i < data.length; i++) {
        const char = data[i];
        if (char === '"' && (i === 0 || data[i - 1] !== '\\')) {
            // toggle quotes when reaching a quote that is the first character
            // or if the character before it is \\
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            // if we reach a comma outside of quotes, 
            // we know it's a column break
            row.push(value);
            value = '';
        } else if (char === '\n' && !inQuotes) {
            // if we reach a line break character outside of quotes,
            // we know it's a line break
            row.push(value);
            rows.push(row);
            row = [];
            value = '';
        } else {
            // continue to build the row
            value += char;
        }
    };

    if (value || row.length > 0) {
        // add any remaining data after loop finishes
        row.push(value);
        rows.push(row);
    };

    return rows;
}


function handleRadioButtonClick(event) {
    const view = event.target.value;
    if (view === 'by-posts') {
        displayDefaultView();
    } else if (view === 'by-users') {
        displayGroupedByUsers();
    };
};


function displayDefaultView() {
    const dataContainer = document.querySelector('#data-container');
    dataContainer.innerHTML = '<p>Loading...</p>';

    setTimeout(() => {
        if (htmlByPost) {
            dataContainer.innerHTML = htmlByPost;
            return;
        }
    
        let html = '<table class="data-table">';

        const header = parsedData[0];
        html += '<thead><tr>';
        header.forEach(column => {
            html += `<th>${column}</th>`;
        })
        html += '</tr></thead><tbody>';
    
        parsedData.forEach((row, rowIndex) => {
            if (rowIndex !== 0) {
                html += `<tr class="default-row" data-row-index="${rowIndex}">`;
                row.forEach((column) => {
                    html += `<td>${column.trim()}</td>`;
                });
                html += '</tr>';
            }
        });
    
        html += '</table>';
        htmlByPost = html;
        dataContainer.innerHTML = html;
    }, 0);
}


function displayGroupedByUsers() {
    const dataContainer = document.querySelector('#data-container');
    dataContainer.innerHTML = '<p>Loading...</p>';

    setTimeout(() => {
        if (htmlByUser) {
            dataContainer.innerHTML = htmlByUser;
            addExpandUserListeners();
            return;
        };
    
        const groupedData = groupByUsers(parsedData);
        const html = generateExpandableHTML(groupedData);
    
        dataContainer.innerHTML = html;
        htmlByUser = html;
    
        addExpandUserListeners();
    }, 0);
}


function groupByUsers(rows) {
    const groupedData = {};
    const header = rows[0];
    const user1Index = header.indexOf('Username_1');
    const user2Index = header.indexOf('Username_2');

    rows.slice(1).forEach(row => {
        const user1 = row[user1Index];
        const user2 = row[user2Index];
        if (!groupedData[user1, user2]) {
            groupedData[user1, user2] = [];
        };
        groupedData[user1, user2].push(row);
    });

    return { header, groupedData };
}


function generateExpandableHTML({ header, groupedData }) {
    let html = '<table class="data-table">';
    html += '<thead><tr>';
    const expandableHeader = header.slice(0, 4);
    expandableHeader.forEach(column => {
        html += `<th>${column}</th>`;
    });
    // empty cell for the expand icon
    html += '<th></th>';
    html += '</tr></thead><tbody>';

    let expandableRowIndex = 0;
    let hiddenRowIndex = 0;

    Object.keys(groupedData).forEach(userPair => {
        const rows = groupedData[userPair];
        const expandableRowClass = expandableRowIndex % 2 === 0 ? 'expandable-row-even' : 'expandable-row-odd';
        html += `<tr class="expandable-row ${expandableRowClass}" data-userPair="${userPair}">`;
        const expandableRowCells = rows[0].slice(0, 4);
        expandableRowCells.forEach(cell => {
            html += `<td>${cell}</td>`;
        });
        html += '<td class="expand-icon-cell"><img class="expand-icon" src="./images/expand.png"></td>';
        html += '</tr>';
        expandableRowIndex++;

        html += `<tr class="hidden-row" data-userPair="${userPair}">`;
            header.forEach(cell => {
                html += `<th>${cell}</th>`;
            });
        html += '</tr>';
        rows.forEach(row => {
            const hidddenRowClass = hiddenRowIndex % 2 === 0 ? 'hidden-row-even' : 'hidden-row-odd';
            html += `<tr class="hidden-row ${hidddenRowClass}" data-userPair="${userPair}">`;
            row.forEach(cell => {
                html += `<td>${cell}</td>`;
            });
            html += '</tr>';
            hiddenRowIndex++;
        });
    });

    html += '</tbody></table>';
    return html;
}


function addExpandUserListeners() {
    document.querySelectorAll('.expandable-row').forEach(row => {
        row.addEventListener('click', () => {
            const expandIcon = row.querySelector('.expand-icon');
            if (expandIcon) {
                const isExpanded = expandIcon.getAttribute('src') === './images/expand.png';
                expandIcon.setAttribute('src', isExpanded ? './images/collapse.png' : './images/expand.png');
            }
            const userPair = row.getAttribute('data-userPair');
            document.querySelectorAll(`tr[data-userPair="${userPair}"]`).forEach(hiddenRow => {
                if (hiddenRow !== row) {
                    hiddenRow.classList.toggle('hidden-row');
                }
            });
        });
    });
}