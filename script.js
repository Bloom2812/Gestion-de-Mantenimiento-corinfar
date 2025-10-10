import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth, signInAnonymously, onAuthStateChanged, signInWithCustomToken } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import { 
    getFirestore, collection, doc, onSnapshot, 
    addDoc, setDoc, deleteDoc, getDocs, query, updateDoc, getDoc, where
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

// --- Firebase Config (placeholders will be populated by environment) ---
const firebaseConfig = typeof __firebase_config !== 'undefined' ? JSON.parse(__firebase_config) : {};
const appId = typeof __app_id !== 'undefined' ? __app_id : 'default-cmms-app';

// --- Firebase Initialization ---
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);
const auth = getAuth(app);

// --- App State ---
const state = {
    currentUser: null,
    currentTab: 'dashboard',
    machines: [],
    parts: [],
    proveedores: [],
    technicians: [],
    workOrders: [],
    solicitudes: [],
    calendarDate: new Date(),
    activeTimers: {},
    collections: {
        machines: collection(db, `/artifacts/${appId}/public/data/machines`),
        parts: collection(db, `/artifacts/${appId}/public/data/parts`),
        proveedores: collection(db, `/artifacts/${appId}/public/data/proveedores`),
        technicians: collection(db, `/artifacts/${appId}/public/data/technicians`),
        workOrders: collection(db, `/artifacts/${appId}/public/data/workOrders`),
        solicitudes: collection(db, `/artifacts/${appId}/public/data/solicitudes`),
    },
    modals: {},
    charts: {},
    searchTimeouts: {}
};

// --- Utility Functions ---
function showToast(message, type = 'info') {
    const toastContainer = document.querySelector('.toast-container');
    const toastId = 'toast-' + Date.now();
    
    const toast = document.createElement('div');
    toast.className = `toast align-items-center toast-${type}`;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    toast.id = toastId;
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;
    
    toastContainer.appendChild(toast);
    
    const bsToast = new bootstrap.Toast(toast, {
        autohide: true,
        delay: 5000
    });
    
    bsToast.show();
    
    toast.addEventListener('hidden.bs.toast', () => {
        toast.remove();
    });
}

function showLoading(show = true) {
    const loadingOverlay = document.getElementById('global-loading');
    if (show) {
        loadingOverlay.classList.remove('d-none');
    } else {
        loadingOverlay.classList.add('d-none');
    }
}

function debounce(func, wait) {
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(state.searchTimeouts[func.name]);
            func(...args);
        };
        clearTimeout(state.searchTimeouts[func.name]);
        state.searchTimeouts[func.name] = setTimeout(later, wait);
    };
}

// --- Initialization ---
document.addEventListener('DOMContentLoaded', async () => {
    if (!firebaseConfig.apiKey) {
        document.body.innerHTML = `<div class="alert alert-danger m-5"><strong>Error de Configuración:</strong> La configuración de Firebase no se ha cargado correctamente. La aplicación no puede iniciarse. Asegúrese de que las variables del entorno estén disponibles.</div>`;
        throw new Error("Firebase config is missing or invalid.");
    }

    // Init Bootstrap Modals
    state.modals.machine = new bootstrap.Modal(document.getElementById('machine-modal'));
    state.modals.part = new bootstrap.Modal(document.getElementById('part-modal'));
    state.modals.proveedor = new bootstrap.Modal(document.getElementById('proveedor-modal'));
    state.modals.technician = new bootstrap.Modal(document.getElementById('technician-modal'));
    state.modals.confirm = new bootstrap.Modal(document.getElementById('confirm-modal'));
    state.modals.workOrder = new bootstrap.Modal(document.getElementById('work-order-modal'));
    state.modals.manageParts = new bootstrap.Modal(document.getElementById('manage-parts-modal'));
    state.modals.solicitud = new bootstrap.Modal(document.getElementById('solicitud-modal'));
    
    setupEventListeners();
    updateCurrentDate();
    populateDateSelectors();
    initCharts();
    handlePeriodChange(); // Set initial state for period selectors
    
    onAuthStateChanged(auth, async (user) => {
        if (user) {
            console.log("Firebase user signed in:", user.uid);
            await initializeSampleData();
            setupRealtimeListeners();
        } else {
             console.log("Firebase user signed out.");
             try {
                 if (typeof __initial_auth_token !== 'undefined' && __initial_auth_token) {
                     await signInWithCustomToken(auth, __initial_auth_token);
                 } else {
                     await signInAnonymously(auth);
                 }
            } catch (error) {
                 console.error("Error during sign-in:", error);
                 showToast('Error al conectar con el servidor', 'error');
            }
        }
    });
});

// --- Sample Data Initialization (Runs once if DB is empty) ---
async function initializeSampleData() {
    showLoading(true);
    try {
        const techniciansSnapshot = await getDocs(state.collections.technicians);
        if (techniciansSnapshot.empty) {
            console.log("No existing technicians found. Initializing administrator account...");
            
            const adminUser = { 
                username: 'admin', 
                password: 'admin', 
                role: 'Admin', 
                permissions: ['all'], 
                salario: 30000
            };

            await addDoc(state.collections.technicians, adminUser);
            
            console.log("Administrator account created.");
            showToast('Cuenta de administrador creada. ¡Bienvenido!', 'success');
        }
    } catch (error) {
        console.error("Error initializing administrator account:", error);
        showToast('Error al crear la cuenta de administrador', 'error');
    } finally {
        showLoading(false);
    }
}

// --- Realtime Listeners ---
function setupRealtimeListeners() {
    // OPTIMIZED: Use docChanges for efficient DOM updates on simple list views
    onSnapshot(query(state.collections.machines), snapshot => {
        let requiresKpiUpdate = false;
        let dataChanged = false;
        snapshot.docChanges().forEach(change => {
            dataChanged = true;
            const machineData = { fb_id: change.doc.id, ...change.doc.data() };
            const index = state.machines.findIndex(m => m.fb_id === change.doc.id);
            requiresKpiUpdate = true;

            if (change.type === "added") {
                if (index === -1) state.machines.push(machineData);
            }
            if (change.type === "modified") {
                if (index > -1) state.machines[index] = machineData;
            }
            if (change.type === "removed") {
                if (index > -1) state.machines.splice(index, 1);
            }
        });

        if (dataChanged) {
            renderMachines();
            populateMachineSelectors();
            
            let machinesToCount = state.machines;
            if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
                const managedIds = new Set(state.currentUser.managedMachineIds);
                machinesToCount = state.machines.filter(m => managedIds.has(m.id));
            }
            document.getElementById('stat-maquinas').textContent = machinesToCount.length;
            
            if (requiresKpiUpdate) updateDashboardData();
        }
    });
    
    onSnapshot(query(state.collections.parts), snapshot => {
        let requiresCostUpdate = false;
        let dataChanged = false;
         snapshot.docChanges().forEach(change => {
            dataChanged = true;
            const partData = { fb_id: change.doc.id, ...change.doc.data() };
            const index = state.parts.findIndex(p => p.fb_id === change.doc.id);
            if (change.type !== "removed") {
                const oldPart = state.parts[index];
                if (!oldPart || oldPart.cost !== partData.cost) requiresCostUpdate = true;
            } else {
                requiresCostUpdate = true;
            }

            if (change.type === "added") {
                if (index === -1) state.parts.push(partData);
            }
            if (change.type === "modified") {
                if (index > -1) state.parts[index] = partData;
            }
            if (change.type === "removed") {
                if (index > -1) state.parts.splice(index, 1);
            }
        });

        if (dataChanged) {
            renderParts();
            checkLowStockNotifications();
            if (requiresCostUpdate) updateDashboardData();
        }
    });

     onSnapshot(query(state.collections.proveedores), snapshot => {
        let dataChanged = false;
         snapshot.docChanges().forEach(change => {
            dataChanged = true;
            const provData = { fb_id: change.doc.id, ...change.doc.data() };
            const index = state.proveedores.findIndex(p => p.fb_id === change.doc.id);

            if (change.type === "added") {
                if (index === -1) state.proveedores.push(provData);
            }
            if (change.type === "modified") {
                if (index > -1) state.proveedores[index] = provData;
            }
            if (change.type === "removed") {
                if (index > -1) state.proveedores.splice(index, 1);
            }
        });

        if (dataChanged) {
            renderProveedores();
            populateSupplierSelectors();
        }
    });
    
    onSnapshot(query(state.collections.technicians), snapshot => {
        const wasEmpty = state.technicians.length === 0;
         snapshot.docChanges().forEach(change => {
            const techData = { fb_id: change.doc.id, ...change.doc.data() };
            const index = state.technicians.findIndex(p => p.fb_id === change.doc.id);

            if (change.type === "added") {
                if (index === -1) state.technicians.push(techData);
            } else if (change.type === "modified") {
                if (index > -1) state.technicians[index] = techData;
            } else if (change.type === "removed") {
                if (index > -1) state.technicians.splice(index, 1);
            }
        });

        renderTechnicians(); // Full re-render is fine for this small/infrequent table

        if (wasEmpty && state.technicians.length > 0) {
            document.getElementById('login-status-text').textContent = 'Ingrese sus credenciales';
            document.getElementById('username').disabled = false;
            document.getElementById('password').disabled = false;
            document.querySelector('#login-form button[type="submit"]').disabled = false;
        }
    });
    
    // For complex views like dashboard, calendar, kanban, a full re-render on data change is more robust and acceptable.
    onSnapshot(query(state.collections.workOrders), snapshot => {
        state.workOrders = snapshot.docs.map(doc => ({ fb_id: doc.id, ...doc.data() }));
        populateWorkOrderSelectors();
        renderCalendar();
        if (state.currentTab === 'trabajo-activo') renderActiveWorkView();
        if (state.currentTab === 'ordenes-asignadas') renderAssignedOrders();
        if (state.currentTab === 'solicitudes') renderSolicitudes(); // Re-render solicitudes as WO status affects them
        updateDashboardData();
    });

    if (state.currentUser) {
        let solicitudesQuery;
        if (state.currentUser.role === 'Operario') {
            solicitudesQuery = query(state.collections.solicitudes, where("requester", "==", state.currentUser.username));
        } else {
            solicitudesQuery = query(state.collections.solicitudes);
        }

        onSnapshot(solicitudesQuery, snapshot => {
            state.solicitudes = snapshot.docs.map(doc => ({ fb_id: doc.id, ...doc.data() }));
            if (state.currentTab === 'solicitudes') renderSolicitudes();
            if (state.currentTab === 'trabajo-activo') renderActiveWorkView();
            updateDashboardData(); // Update pending requests stat
        });
    }
}

function checkLowStockNotifications() {
    if (!state.currentUser || state.currentUser.role !== 'Admin') return;
    
    const lowStockParts = state.parts.filter(part => part.stock <= part.minStock);
    
    lowStockParts.forEach(part => {
        showToast(`Stock bajo para ${part.description}. Actual: ${part.stock}, Mínimo: ${part.minStock}`, 'warning');
    });
}

// --- Event Listeners Setup ---
function setupEventListeners() {
    document.getElementById('login-form').addEventListener('submit', handleLogin);
    document.getElementById('password-toggle').addEventListener('click', togglePasswordVisibility);
    
    document.getElementById('logout-btn').addEventListener('click', handleLogout);
    
    document.querySelectorAll('.nav-tab').forEach(tab => {
        tab.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(tab.dataset.tab);
            if(window.innerWidth < 992) {
                toggleSidebar(false);
            }
        });
    });

    document.getElementById('sidebar-toggle').addEventListener('click', () => toggleSidebar());
    document.getElementById('sidebar-overlay').addEventListener('click', () => toggleSidebar(false));
    
    // Machine Listeners
    document.getElementById('add-machine-btn').addEventListener('click', () => showMachineModal());
    document.getElementById('machine-form').addEventListener('submit', handleMachineSubmit);
    document.getElementById('search-machine-input').addEventListener('input', debounce(renderMachines, 300));

    document.getElementById('disable-schedule-check').addEventListener('change', e => {
        document.getElementById('schedule-wrapper').style.display = e.target.checked ? 'none' : 'block';
    });

    // Part Listeners
    document.getElementById('add-part-btn').addEventListener('click', () => showPartModal());
    document.getElementById('part-form').addEventListener('submit', handlePartSubmit);
    document.getElementById('search-part-input').addEventListener('input', debounce(renderParts, 300));

    // Proveedor Listeners
    document.getElementById('add-proveedor-btn').addEventListener('click', () => showProveedorModal());
    document.getElementById('proveedor-form').addEventListener('submit', handleProveedorSubmit);
    document.getElementById('search-proveedor-input').addEventListener('input', debounce(renderProveedores, 300));

    // Technician/User Listeners
    document.getElementById('add-technician-btn').addEventListener('click', () => showTechnicianModal());
    document.getElementById('technician-form').addEventListener('submit', handleTechnicianSubmit);
    document.getElementById('technician-role').addEventListener('change', (e) => {
        updateTechnicianModalUIForRole(e.target.value);
    });
    
    // Solicitud Listeners
    document.getElementById('add-solicitud-btn').addEventListener('click', showSolicitudModal);
    document.getElementById('solicitud-form').addEventListener('submit', handleSolicitudSubmit);

    // Planner Listeners
    document.getElementById('prev-month-btn').addEventListener('click', () => changeMonth(-1));
    document.getElementById('next-month-btn').addEventListener('click', () => changeMonth(1));
    document.getElementById('add-preventive-task-btn').addEventListener('click', () => showWorkOrderModal(null, 'Preventivo'));
    document.getElementById('add-corrective-task-btn-planner').addEventListener('click', () => showWorkOrderModal(null, 'Correctivo'));

    // Work Order Listeners
    document.getElementById('wo-type').addEventListener('change', handleWorkOrderTypeChange);
    document.getElementById('wo-save-btn').addEventListener('click', handleWorkOrderSaveClick);
    document.getElementById('wo-start-btn').addEventListener('click', () => saveWorkOrder(document.getElementById('wo-id').value.trim(), { status: 'En Proceso' }));
    document.getElementById('wo-pause-btn').addEventListener('click', () => saveWorkOrder(document.getElementById('wo-id').value.trim(), { status: 'Pausado' }));
    document.getElementById('wo-resume-btn').addEventListener('click', () => saveWorkOrder(document.getElementById('wo-id').value.trim(), { status: 'En Proceso' }));
    document.getElementById('wo-complete-btn').addEventListener('click', () => saveWorkOrder(document.getElementById('wo-id').value.trim(), { status: 'Completado' }));
    document.getElementById('wo-add-part-btn').addEventListener('click', () => addPartToWorkOrder());
    document.getElementById('wo-add-support-technician-btn').addEventListener('click', handleAddSupportTechnicianClick);
    document.getElementById('wo-lead-technician-select').addEventListener('change', handleLeadTechnicianChange);

    // Manage Parts Modal Listeners
    document.getElementById('add-update-part-btn').addEventListener('click', handleAddOrUpdatePartInModal);
    document.getElementById('save-managed-parts-btn').addEventListener('click', handleSaveManagedParts);

    // Report Listeners
    document.getElementById('generate-general-report-btn').addEventListener('click', generateGeneralReport);
    document.getElementById('generate-wo-report-btn').addEventListener('click', generateWorkOrderReport);
    document.getElementById('generate-machine-report-btn').addEventListener('click', generateMachineReport);
    document.getElementById('generate-machine-parts-report-btn').addEventListener('click', generateMachinePartsReport);
    document.getElementById('generate-single-wo-report-btn').addEventListener('click', generateSingleWorkOrderReport);

    // Dashboard Filters
    document.getElementById('dashboardDate').addEventListener('change', updateDashboardData);
    document.getElementById('dashboardWeek').addEventListener('change', updateDashboardData);
    document.getElementById('dashboard-period-select').addEventListener('change', handlePeriodChange);
    document.getElementById('dashboardMonth').addEventListener('change', () => {
        if (document.getElementById('dashboard-period-select').value === 'week') {
            populateWeekSelector();
        }
        updateDashboardData();
    });
    document.getElementById('dashboardYear').addEventListener('change', () => {
        if (document.getElementById('dashboard-period-select').value === 'week') {
            populateWeekSelector();
        }
        updateDashboardData();
    });
    document.getElementById('kpi-machine-select').addEventListener('change', updateDashboardData);
    
    const statusIndicator = document.getElementById('connection-status-indicator');
    window.addEventListener('online', () => updateConnectionStatus(true));
    window.addEventListener('offline', () => updateConnectionStatus(false));
    updateConnectionStatus(navigator.onLine);
}

// --- Auth & Permissions Functions ---
function handleLogin(e) {
    e.preventDefault();
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    
    document.getElementById('login-text').classList.add('d-none');
    document.getElementById('login-spinner').classList.remove('d-none');
    document.getElementById('login-container').classList.add('loading');
    
    const user = state.technicians.find(t => t.username === username && t.password === password);
    
    if (user) {
        state.currentUser = user;
        document.getElementById('login-overlay').classList.add('d-none');
        document.getElementById('app-wrapper').classList.remove('d-none');
        
        document.getElementById('user-display').textContent = user.username;
        document.getElementById('user-role').textContent = user.role;
        document.getElementById('user-avatar').src = `https://ui-avatars.com/api/?name=${user.username}&background=0D8ABC&color=fff`;
        
        document.getElementById('login-error').classList.add('d-none');
        
        showToast(`Bienvenido, ${user.username}`, 'success');
        
        applyUserPermissions();
        setupRealtimeListeners(); // Re-setup listeners with correct user role
    } else {
        document.getElementById('login-error').classList.remove('d-none');
        showToast('Usuario o contraseña incorrectos', 'error');
    }
    
    document.getElementById('login-text').classList.remove('d-none');
    document.getElementById('login-spinner').classList.add('d-none');
    document.getElementById('login-container').classList.remove('loading');
}

function handleLogout() {
    showConfirmation('Cerrar sesión', '¿Está seguro de que desea cerrar la sesión?', () => {
        window.location.reload();
    });
}

function applyUserPermissions() {
    if (!state.currentUser) return;

    const { role, permissions = [] } = state.currentUser;
    const allTabs = document.querySelectorAll('.nav-tab');

    allTabs.forEach(tab => {
         tab.style.display = 'none';
    });
     document.querySelector('.nav-tab[data-tab="dashboard"]').style.display = 'flex';


    let visibleTabs = [];

    switch (role) {
        case 'Admin':
            visibleTabs = ['maquinaria', 'repuestos', 'proveedores', 'tecnicos', 'trabajo-activo', 'planificador', 'reportes', 'solicitudes'];
            break;
        case 'Jefe de Area':
            visibleTabs = permissions || [];
            break;
        case 'Invitado':
            visibleTabs = ['maquinaria', 'repuestos', 'proveedores', 'tecnicos', 'trabajo-activo', 'planificador', 'reportes'];
            break;
        case 'Técnico':
            visibleTabs = permissions.includes('all') 
                ? ['maquinaria', 'repuestos', 'proveedores', 'planificador', 'ordenes-asignadas', 'reportes'] 
                : permissions;
            if(permissions.includes('ordenes-asignadas')) {
                 visibleTabs.push('trabajo-activo');
            }
            break;
        case 'Operario':
            visibleTabs = ['solicitudes'];
            break;
    }

    visibleTabs.forEach(tabName => {
        const tab = document.querySelector(`.nav-tab[data-tab="${tabName}"]`);
        if (tab) tab.style.display = 'flex';
    });

     const canManageSystem = role === 'Admin';
     const canPlan = role === 'Admin'; // Cambiado: Jefe de Area ya no puede planificar, solo ver.

     document.getElementById('add-machine-btn').style.display = canManageSystem ? 'inline-block' : 'none';
     document.getElementById('add-part-btn').style.display = canManageSystem ? 'inline-block' : 'none';
     document.getElementById('add-proveedor-btn').style.display = canManageSystem ? 'inline-block' : 'none';
     document.getElementById('add-technician-btn').style.display = canManageSystem ? 'inline-block' : 'none';
     document.getElementById('add-preventive-task-btn').style.display = canPlan ? 'inline-block' : 'none';
     document.getElementById('add-corrective-task-btn-planner').style.display = canPlan ? 'inline-block' : 'none';

    if(role === 'Invitado') {
        document.querySelectorAll('button[id^="add-"]').forEach(btn => btn.style.display = 'none');
    }

    renderMachines();
    renderParts();
    renderProveedores();
    renderTechnicians();
}


// --- UI Functions ---
function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.add('d-none'));
    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
    
    document.getElementById(tabName).classList.remove('d-none');
    const activeTab = document.querySelector(`[data-tab="${tabName}"]`);
    activeTab.classList.add('active');
    
    document.getElementById('page-title').textContent = activeTab.innerText.trim();
    state.currentTab = tabName;

    document.getElementById('dashboard-date-filter').classList.toggle('d-none', tabName !== 'dashboard');


    if (tabName === 'trabajo-activo') renderActiveWorkView();
    if (tabName === 'ordenes-asignadas') renderAssignedOrders();
    if (tabName === 'planificador') renderCalendar();
    if (tabName === 'solicitudes') renderSolicitudes();
}

function togglePasswordVisibility() {
    const passwordInput = document.getElementById('password');
    const icon = document.getElementById('password-toggle').querySelector('i');
    
    if (passwordInput.type === 'password') {
        passwordInput.type = 'text';
        icon.classList.replace('fa-eye', 'fa-eye-slash');
    } else {
        passwordInput.type = 'password';
        icon.classList.replace('fa-eye-slash', 'fa-eye');
    }
}

function toggleSidebar(forceState) {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    const mainContent = document.querySelector('.main-content');
    
    const show = forceState !== undefined ? forceState : !sidebar.classList.contains('show');

    if (show) {
        sidebar.classList.add('show');
        overlay.style.display = 'block';
        if(window.innerWidth < 992) {
           mainContent.style.marginLeft = '260px'; 
        }
    } else {
        sidebar.classList.remove('show');
        overlay.style.display = 'none';
        mainContent.style.marginLeft = '0';
    }
}

function showConfirmation(title, message, onConfirm) {
    document.getElementById('confirm-modal-title').textContent = title;
    document.getElementById('confirm-modal-body').textContent = message;
    
    const confirmBtn = document.getElementById('confirm-modal-btn');
    const newConfirmBtn = confirmBtn.cloneNode(true);
    confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
    
    const modalInstance = new bootstrap.Modal(document.getElementById('confirm-modal'));

    newConfirmBtn.addEventListener('click', () => {
        onConfirm();
        modalInstance.hide();
    });
    
    modalInstance.show();
}

function updateConnectionStatus(online) {
    const statusIndicator = document.getElementById('connection-status-indicator');
    state.isOnline = online;
    
    if (online) {
        statusIndicator.classList.remove('offline');
        statusIndicator.classList.remove('syncing');
        statusIndicator.title = 'Conectado';
        if (state.currentUser) showToast('Conexión restablecida', 'success');
    } else {
        statusIndicator.classList.remove('syncing');
        statusIndicator.classList.add('offline');
        statusIndicator.title = 'Sin conexión';
        if (state.currentUser) showToast('Se perdió la conexión. Algunas funciones pueden no estar disponibles.', 'warning');
    }
}

function updateCurrentDate() {
    const options = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
    const currentDate = new Date().toLocaleDateString('es-ES', options);
    document.getElementById('current-date').textContent = currentDate.charAt(0).toUpperCase() + currentDate.slice(1);
}

function populateDateSelectors() {
    const monthSelect = document.getElementById('dashboardMonth');
    const yearSelect = document.getElementById('dashboardYear');
    const dateSelect = document.getElementById('dashboardDate');
    const now = new Date();
    const currentMonth = now.getMonth();
    const currentYear = now.getFullYear();

    monthSelect.innerHTML = '';
    const months = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];
    months.forEach((month, index) => {
        const option = document.createElement('option');
        option.value = index;
        option.textContent = month;
        if (index === currentMonth) {
            option.selected = true;
        }
        monthSelect.appendChild(option);
    });

    yearSelect.innerHTML = '';
    for (let i = currentYear + 1; i >= currentYear - 5; i--) {
        const option = document.createElement('option');
        option.value = i;
        option.textContent = i;
        if (i === currentYear) {
            option.selected = true;
        }
        yearSelect.appendChild(option);
    }

    dateSelect.value = now.toISOString().split('T')[0];
}

function populateWeekSelector() {
    const weekSelect = document.getElementById('dashboardWeek');
    const month = parseInt(document.getElementById('dashboardMonth').value);
    const year = parseInt(document.getElementById('dashboardYear').value);
    weekSelect.innerHTML = '';

    const firstOfMonth = new Date(year, month, 1);
    const lastOfMonth = new Date(year, month + 1, 0);

    let weekStart = new Date(firstOfMonth);
    weekStart.setDate(weekStart.getDate() - weekStart.getDay()); // Start from Sunday of the first week

    let weekCounter = 1;
    while (weekStart <= lastOfMonth) {
        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        const option = document.createElement('option');
        option.value = `${weekStart.toISOString().split('T')[0]}|${weekEnd.toISOString().split('T')[0]}`;
        option.textContent = `Semana ${weekCounter}: ${weekStart.toLocaleDateString('es-ES')} - ${weekEnd.toLocaleDateString('es-ES')}`;
        weekSelect.appendChild(option);

        weekStart.setDate(weekStart.getDate() + 7);
        weekCounter++;
    }
}

function initCharts() {
    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
            },
        }
    };

    state.charts.maintenance = new Chart(document.getElementById('maintenanceChart').getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: ['Preventivo', 'Correctivo'],
            datasets: [{
                label: 'Tipos de Mantenimiento',
                data: [0, 0],
                backgroundColor: ['#3498db', '#e74c3c'],
            }]
        },
        options: { ...chartOptions, plugins: { ...chartOptions.plugins, title: { display: true, text: 'Tipos de Mantenimiento' }}}
    });
    
    state.charts.failureType = new Chart(document.getElementById('failureTypeChart').getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['Mecánica', 'Eléctrica', 'Electrónica', 'Operación'],
            datasets: [{
                label: 'Tipos de Falla',
                data: [0, 0, 0, 0],
                backgroundColor: '#8e44ad',
            }]
        },
        options: { ...chartOptions, indexAxis: 'y', scales: { y: { beginAtZero: true }}, plugins: { ...chartOptions.plugins, title: { display: true, text: 'Análisis de Fallas Correctivas' }}}
    });

    state.charts.taskStatus = new Chart(document.getElementById('taskStatusChart').getContext('2d'), {
        type: 'pie',
        data: {
            labels: ['Pendiente', 'En Proceso', 'Completado', 'Cancelado'],
            datasets: [{
                label: 'Estado de Tareas',
                data: [0, 0, 0, 0],
                backgroundColor: ['#f39c12', '#3498db', '#2ecc71', '#e74c3c'],
            }]
        },
        options: { ...chartOptions, plugins: { ...chartOptions.plugins, title: { display: true, text: 'Estado de Órdenes de Trabajo' }}}
    });

    state.charts.correctiveTrends = new Chart(document.getElementById('correctiveTrendsChart').getContext('2d'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Preventivos',
                    data: [],
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: true,
                    tension: 0.3
                },
                {
                    label: 'Correctivos',
                    data: [],
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: true,
                    tension: 0.3
                }
            ]
        },
        options: { ...chartOptions, plugins: { ...chartOptions.plugins, title: { display: true, text: 'Tendencia Mensual (Últimos 12 Meses)' }}}
    });
}

// --- NEW: Generic function to remove a table row by ID ---
function removeTableRow(rowId) {
    const row = document.getElementById(rowId);
    if (row) row.remove();
}

// --- NEW: HTML creation helpers for table rows ---
function createMachineRowHTML(machine) {
    let actionsHTML = 'Sin permisos';
    if(state.currentUser?.role === 'Invitado') {
        actionsHTML = `<span class="text-muted fst-italic">Solo lectura</span>`;
    } else if (state.currentUser?.role === 'Admin' || state.currentUser?.permissions.includes('all') || state.currentUser?.permissions.includes('maquinaria')) {
        actionsHTML = `
            <button class="btn btn-sm btn-outline-primary edit-machine" data-id="${machine.id}"><i class="fas fa-edit"></i></button> 
            <button class="btn btn-sm btn-outline-danger delete-machine" data-id="${machine.id}"><i class="fas fa-trash"></i></button>
        `;
    }
    const typeText = machine.type === 'instalacion' ? 'Instalación' : 'Máquina';
    return `<td>${machine.id}</td><td>${machine.name}</td><td>${typeText}</td><td>${machine.location}</td><td class="text-center">${actionsHTML}</td>`;
}

function createPartRowHTML(part) {
    let actionsHTML = 'Sin permisos';
    if(state.currentUser?.role === 'Invitado') {
        actionsHTML = `<span class="text-muted fst-italic">Solo lectura</span>`;
    } else if (state.currentUser?.role === 'Admin' || state.currentUser?.permissions.includes('all') || state.currentUser?.permissions.includes('repuestos')) {
        actionsHTML = `
            <button class="btn btn-sm btn-outline-primary edit-part" data-id="${part.id}"><i class="fas fa-edit"></i></button> 
            <button class="btn btn-sm btn-outline-danger delete-part" data-id="${part.id}"><i class="fas fa-trash"></i></button>
        `;
    }

    const supplier = state.proveedores.find(s => s.id === part.supplierId);
    let machineNames = 'N/A';
    const machineIds = part.machineIds || (part.machineId ? [part.machineId] : []);
    if (machineIds.length > 0) {
        if (machineIds.length === 1) {
            const machine = state.machines.find(m => m.id === machineIds[0]);
            machineNames = machine ? machine.name : machineIds[0];
        } else {
            machineNames = `<span class="badge bg-secondary">${machineIds.length} Máquinas Vinculadas</span>`;
        }
    }
    
    const classificationText = part.classification === 'insumo' ? 'Insumo' : 'Repuesto';

    return `
        <td>${part.id}</td><td>${part.description}</td>
        <td><span class="badge bg-${part.classification === 'insumo' ? 'info' : 'secondary'}">${classificationText}</span></td>
        <td>${supplier ? supplier.nombre : 'N/A'}</td>
        <td>${machineNames}</td><td>$${part.cost.toFixed(2)}</td><td>${part.stock}</td>
        <td>${part.minStock}</td><td>${part.location || 'N/A'}</td>
        <td class="text-center">${actionsHTML}</td>
    `;
}

function createProveedorRowHTML(proveedor) {
    let actionsHTML = 'Sin permisos';
    if(state.currentUser?.role === 'Invitado') {
        actionsHTML = `<span class="text-muted fst-italic">Solo lectura</span>`;
    } else if (state.currentUser?.role === 'Admin' || state.currentUser?.permissions.includes('all') || state.currentUser?.permissions.includes('proveedores')) {
        actionsHTML = `
            <button class="btn btn-sm btn-outline-primary edit-proveedor" data-id="${proveedor.id}"><i class="fas fa-edit"></i></button> 
            <button class="btn btn-sm btn-outline-danger delete-proveedor" data-id="${proveedor.id}"><i class="fas fa-trash"></i></button>
        `;
    }
    return `
        <td>${proveedor.id}</td><td>${proveedor.nombre}</td><td>${proveedor.rtn || 'N/A'}</td>
        <td>${proveedor.telefono || 'N/A'}</td><td>${proveedor.direccion || 'N/A'}</td>
        <td>${proveedor.email || 'N/A'}</td><td class="text-center">${actionsHTML}</td>
    `;
}

// --- CRUD Machine Functions ---
function renderMachines() {
    const tbody = document.getElementById('machine-list');
    const searchTerm = document.getElementById('search-machine-input').value.toLowerCase();
    tbody.innerHTML = '';

    let machinesToRender = state.machines;
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        machinesToRender = state.machines.filter(m => managedIds.has(m.id));
    }

    const filteredMachines = machinesToRender.filter(m => m.id.toLowerCase().includes(searchTerm) || m.name.toLowerCase().includes(searchTerm));
    if (filteredMachines.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center">No se encontraron máquinas.</td></tr>';
        return;
    }
    
    const fragment = document.createDocumentFragment();
    filteredMachines.forEach(machine => {
        const tr = document.createElement('tr');
        tr.id = `machine-row-${machine.fb_id}`;
        tr.innerHTML = createMachineRowHTML(machine);
        fragment.appendChild(tr);
    });
    tbody.appendChild(fragment);

    // Re-attach listeners after full render/filter
    tbody.querySelectorAll('.edit-machine').forEach(btn => btn.addEventListener('click', () => showMachineModal(btn.dataset.id)));
    tbody.querySelectorAll('.delete-machine').forEach(btn => btn.addEventListener('click', () => deleteMachine(btn.dataset.id)));
}

// --- NEW: Incremental DOM update functions for Machines ---
function addOrUpdateMachineInTable(machine) {
    const tbody = document.getElementById('machine-list');
    let tr = document.getElementById(`machine-row-${machine.fb_id}`);
    if (tr) { // Update existing row
        tr.innerHTML = createMachineRowHTML(machine);
    } else { // Add new row
        const placeholder = tbody.querySelector('td[colspan="4"]');
        if (placeholder) placeholder.parentElement.remove();
        tr = document.createElement('tr');
        tr.id = `machine-row-${machine.fb_id}`;
        tr.innerHTML = createMachineRowHTML(machine);
        tbody.appendChild(tr);
    }
    // Attach/Re-attach listeners for the new/updated row
    tr.querySelector('.edit-machine')?.addEventListener('click', () => showMachineModal(tr.querySelector('.edit-machine').dataset.id));
    tr.querySelector('.delete-machine')?.addEventListener('click', () => deleteMachine(tr.querySelector('.delete-machine').dataset.id));
}

function showMachineModal(machineId = null) {
    const form = document.getElementById('machine-form');
    form.reset();
    const idInput = document.getElementById('machine-id');
    const scheduleWrapper = document.getElementById('schedule-wrapper');
    const disableScheduleCheck = document.getElementById('disable-schedule-check');
    
    document.querySelectorAll('.day-check').forEach(c => c.checked = false);
    document.getElementById('machine-start-time-weekday').value = '';
    document.getElementById('machine-end-time-weekday').value = '';
    document.getElementById('machine-start-time-saturday').value = '';
    document.getElementById('machine-end-time-saturday').value = '';
    document.getElementById('machine-start-time-sunday').value = '';
    document.getElementById('machine-end-time-sunday').value = '';

    if (machineId) {
        document.getElementById('machine-modal-title').textContent = 'Editar Máquina';
        const machine = state.machines.find(m => m.id === machineId);
        if (machine) {
            document.getElementById('machine-id-hidden').value = machine.id;
            idInput.value = machine.id;
            document.getElementById('machine-name').value = machine.name;
            document.getElementById('machine-type').value = machine.type || 'maquina';
            document.getElementById('machine-location').value = machine.location;
            idInput.setAttribute('readonly', true);

            const scheduleDisabled = machine.scheduleDisabled || false;
            disableScheduleCheck.checked = scheduleDisabled;
            scheduleWrapper.style.display = scheduleDisabled ? 'none' : 'block';

            if (machine.schedule) {
                if (machine.schedule.weekday || machine.schedule.saturday || machine.schedule.sunday) {
                    if (machine.schedule.weekday) {
                        machine.schedule.weekday.activeDays?.forEach(day => {
                            const checkbox = document.querySelector(`.day-check[value="${day}"]`);
                            if (checkbox) checkbox.checked = true;
                        });
                        document.getElementById('machine-start-time-weekday').value = machine.schedule.weekday.startTime || '';
                        document.getElementById('machine-end-time-weekday').value = machine.schedule.weekday.endTime || '';
                    }
                    if (machine.schedule.saturday?.active) {
                        document.getElementById('day-sat').checked = true;
                        document.getElementById('machine-start-time-saturday').value = machine.schedule.saturday.startTime || '';
                        document.getElementById('machine-end-time-saturday').value = machine.schedule.saturday.endTime || '';
                    }
                    if (machine.schedule.sunday?.active) {
                        document.getElementById('day-sun').checked = true;
                        document.getElementById('machine-start-time-sunday').value = machine.schedule.sunday.startTime || '';
                        document.getElementById('machine-end-time-sunday').value = machine.schedule.sunday.endTime || '';
                    }
                }
            }
        }
    } else {
        document.getElementById('machine-modal-title').textContent = 'Añadir Máquina';
        document.getElementById('machine-id-hidden').value = '';
        disableScheduleCheck.checked = false;
        scheduleWrapper.style.display = 'block';
        document.getElementById('machine-type').value = 'maquina';
        idInput.removeAttribute('readonly');
    }
    state.modals.machine.show();
}

async function handleMachineSubmit(e) {
    e.preventDefault();
    const originalId = document.getElementById('machine-id-hidden').value;
    
    const weekdayActiveDays = [];
    document.querySelectorAll('#machine-form .day-check:checked').forEach(c => {
        const day = parseInt(c.value);
        if (day >= 1 && day <= 5) {
            weekdayActiveDays.push(c.value);
        }
    });

    const schedule = {
        weekday: {
            activeDays: weekdayActiveDays,
            startTime: document.getElementById('machine-start-time-weekday').value,
            endTime: document.getElementById('machine-end-time-weekday').value,
        },
        saturday: {
            active: document.getElementById('day-sat').checked,
            startTime: document.getElementById('machine-start-time-saturday').value,
            endTime: document.getElementById('machine-end-time-saturday').value,
        },
        sunday: {
            active: document.getElementById('day-sun').checked,
            startTime: document.getElementById('machine-start-time-sunday').value,
            endTime: document.getElementById('machine-end-time-sunday').value,
        }
    };

    const machineData = {
        id: document.getElementById('machine-id').value.trim(),
        name: document.getElementById('machine-name').value,
        type: document.getElementById('machine-type').value,
        location: document.getElementById('machine-location').value,
        scheduleDisabled: document.getElementById('disable-schedule-check').checked,
        schedule: schedule
    };
    if(!machineData.id) {
        showToast('El ID de la máquina no puede estar vacío.', 'error');
        return;
    }
    
    try {
        const docRef = doc(state.collections.machines, machineData.id);
        await setDoc(docRef, machineData, { merge: true });
        if (originalId && originalId !== machineData.id) {
             await deleteDoc(doc(state.collections.machines, originalId));
        }
        showToast('Máquina guardada correctamente', 'success');
    } catch (error) { 
        console.error("Error saving machine: ", error);
        showToast('Error al guardar la máquina', 'error');
    }
    state.modals.machine.hide();
}

function deleteMachine(machineId) {
     showConfirmation('Eliminar Máquina', `¿Está seguro de que desea eliminar la máquina ${machineId}?`, async () => {
        try { 
            await deleteDoc(doc(state.collections.machines, machineId)); 
            showToast('Máquina eliminada correctamente', 'success');
        } catch (error) { 
            console.error("Error deleting machine: ", error);
            showToast('Error al eliminar la máquina', 'error');
        }
    });
}

// --- CRUD Part Functions ---
function renderParts() {
    const tbody = document.getElementById('part-list');
    const searchTerm = document.getElementById('search-part-input').value.toLowerCase();
    tbody.innerHTML = '';

    let partsToRender = state.parts;
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        partsToRender = state.parts.filter(p => {
            const machineIds = p.machineIds || (p.machineId ? [p.machineId] : []);
            if (!machineIds || machineIds.length === 0) return false;
            return machineIds.some(machineId => managedIds.has(machineId));
        });
    }

    const filteredParts = partsToRender.filter(p => p.id.toLowerCase().includes(searchTerm) || p.description.toLowerCase().includes(searchTerm));
    if (filteredParts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" class="text-center">No se encontraron repuestos.</td></tr>';
        return;
    }

    const fragment = document.createDocumentFragment();
    filteredParts.forEach(part => {
        const tr = document.createElement('tr');
        tr.id = `part-row-${part.fb_id}`;
        tr.className = part.stock <= part.minStock ? 'table-danger' : '';
        tr.innerHTML = createPartRowHTML(part);
        fragment.appendChild(tr);
    });
    tbody.appendChild(fragment);
    
    tbody.querySelectorAll('.edit-part').forEach(btn => btn.addEventListener('click', () => showPartModal(btn.dataset.id)));
    tbody.querySelectorAll('.delete-part').forEach(btn => btn.addEventListener('click', () => deletePart(btn.dataset.id)));
}

function addOrUpdatePartInTable(part) {
    const tbody = document.getElementById('part-list');
    let tr = document.getElementById(`part-row-${part.fb_id}`);
    if (tr) { // Update
        tr.innerHTML = createPartRowHTML(part);
    } else { // Add
        const placeholder = tbody.querySelector('td[colspan="9"]');
        if (placeholder) placeholder.parentElement.remove();
        tr = document.createElement('tr');
        tr.id = `part-row-${part.fb_id}`;
        tr.innerHTML = createPartRowHTML(part);
        tbody.appendChild(tr);
    }
    tr.className = part.stock <= part.minStock ? 'table-danger' : '';
    tr.querySelector('.edit-part')?.addEventListener('click', () => showPartModal(tr.querySelector('.edit-part').dataset.id));
    tr.querySelector('.delete-part')?.addEventListener('click', () => deletePart(tr.querySelector('.delete-part').dataset.id));
}

function showPartModal(partId = null) {
    const form = document.getElementById('part-form');
    form.reset();
    populateSupplierSelectors();
    const idInput = document.getElementById('part-id');

    const machinesListContainer = document.getElementById('part-machines-list');
    machinesListContainer.innerHTML = '';
    if (state.machines.length > 0) {
        state.machines.forEach(machine => {
            const div = document.createElement('div');
            div.className = 'form-check';
            div.innerHTML = `
                <input class="form-check-input part-machine-check" type="checkbox" value="${machine.id}" id="part-machine-check-${machine.id}">
                <label class="form-check-label" for="part-machine-check-${machine.id}">
                    ${machine.name} (${machine.id})
                </label>
            `;
            machinesListContainer.appendChild(div);
        });
    } else {
        machinesListContainer.innerHTML = '<p class="text-muted small">No hay máquinas registradas.</p>';
    }
    
    if (partId) {
        document.getElementById('part-modal-title').textContent = 'Editar Repuesto';
        const part = state.parts.find(p => p.id === partId);
        if (part) {
            document.getElementById('part-id-hidden').value = part.id;
            idInput.value = part.id;
            document.getElementById('part-description').value = part.description;
            document.getElementById('part-classification').value = part.classification || 'repuesto';
            document.getElementById('part-supplier').value = part.supplierId || '';
            
            const machineIdsToSelect = part.machineIds || (part.machineId ? [part.machineId] : []);
            machineIdsToSelect.forEach(machineId => {
                const checkbox = document.getElementById(`part-machine-check-${machineId}`);
                if (checkbox) {
                    checkbox.checked = true;
                }
            });

            document.getElementById('part-cost').value = part.cost;
            document.getElementById('part-stock').value = part.stock;
            document.getElementById('part-minStock').value = part.minStock;
            document.getElementById('part-location').value = part.location || '';
            idInput.setAttribute('readonly', true);
        }
    } else {
        document.getElementById('part-modal-title').textContent = 'Añadir Repuesto';
        document.getElementById('part-id-hidden').value = '';
        idInput.removeAttribute('readonly');
        document.getElementById('part-classification').value = 'repuesto';
    }
    state.modals.part.show();
}

async function handlePartSubmit(e) {
    e.preventDefault();
    const originalId = document.getElementById('part-id-hidden').value;

    const selectedMachineIds = [];
    document.querySelectorAll('#part-machines-list .part-machine-check:checked').forEach(checkbox => {
        selectedMachineIds.push(checkbox.value);
    });

    const partData = {
        id: document.getElementById('part-id').value.trim(),
        description: document.getElementById('part-description').value,
        classification: document.getElementById('part-classification').value,
        supplierId: document.getElementById('part-supplier').value,
        machineIds: selectedMachineIds,
        cost: parseFloat(document.getElementById('part-cost').value),
        stock: parseInt(document.getElementById('part-stock').value),
        minStock: parseInt(document.getElementById('part-minStock').value),
        location: document.getElementById('part-location').value,
    };
    delete partData.machineId;

    if(!partData.id) {
        showToast('El ID del repuesto no puede estar vacío.', 'error');
        return;
    }
    
    try {
        const docRef = doc(state.collections.parts, partData.id);
        await setDoc(docRef, partData, { merge: true });
        if (originalId && originalId !== partData.id) {
             await deleteDoc(doc(state.collections.parts, originalId));
        }
        showToast('Repuesto guardado correctamente', 'success');
    } catch (error) { 
        console.error("Error saving part: ", error);
        showToast('Error al guardar el repuesto', 'error');
    }
    state.modals.part.hide();
}

function deletePart(partId) {
    showConfirmation('Eliminar Repuesto', `¿Está seguro de que desea eliminar el repuesto ${partId}?`, async () => {
        try { 
            await deleteDoc(doc(state.collections.parts, partId)); 
            showToast('Repuesto eliminado correctamente', 'success');
        } catch (error) { 
            console.error("Error deleting part: ", error);
            showToast('Error al eliminar el repuesto', 'error');
        }
    });
}

// --- CRUD Proveedor Functions ---
function renderProveedores() {
    const tbody = document.getElementById('proveedor-list');
    const searchTerm = document.getElementById('search-proveedor-input').value.toLowerCase();
    tbody.innerHTML = '';
    
    let proveedoresToRender = state.proveedores;

    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedMachineIds = new Set(state.currentUser.managedMachineIds);

        // Find all parts associated with the managed machines
        const relevantParts = state.parts.filter(p => {
            const machineIds = p.machineIds || [];
            return machineIds.some(mId => managedMachineIds.has(mId));
        });

        // Get the unique supplier IDs from those parts
        const relevantSupplierIds = new Set(relevantParts.map(p => p.supplierId).filter(Boolean));

        // Filter the suppliers list
        proveedoresToRender = state.proveedores.filter(prov => relevantSupplierIds.has(prov.id));
    }

    const filtered = proveedoresToRender.filter(p => p.id.toLowerCase().includes(searchTerm) || p.nombre.toLowerCase().includes(searchTerm));
    
    if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center">No se encontraron proveedores.</td></tr>';
        return;
    }

    const fragment = document.createDocumentFragment();
    filtered.forEach(p => {
        const tr = document.createElement('tr');
        tr.id = `proveedor-row-${p.fb_id}`;
        tr.innerHTML = createProveedorRowHTML(p);
        fragment.appendChild(tr);
    });
    tbody.appendChild(fragment);

    tbody.querySelectorAll('.edit-proveedor').forEach(btn => btn.addEventListener('click', () => showProveedorModal(btn.dataset.id)));
    tbody.querySelectorAll('.delete-proveedor').forEach(btn => btn.addEventListener('click', () => deleteProveedor(btn.dataset.id)));
}

function addOrUpdateProveedorInTable(proveedor) {
    const tbody = document.getElementById('proveedor-list');
    let tr = document.getElementById(`proveedor-row-${proveedor.fb_id}`);

    if (tr) { // Update
        tr.innerHTML = createProveedorRowHTML(proveedor);
    } else { // Add
        const placeholder = tbody.querySelector('td[colspan="7"]');
        if (placeholder) placeholder.parentElement.remove();
        tr = document.createElement('tr');
        tr.id = `proveedor-row-${proveedor.fb_id}`;
        tr.innerHTML = createProveedorRowHTML(proveedor);
        tbody.appendChild(tr);
    }
    tr.querySelector('.edit-proveedor')?.addEventListener('click', () => showProveedorModal(tr.querySelector('.edit-proveedor').dataset.id));
    tr.querySelector('.delete-proveedor')?.addEventListener('click', () => deleteProveedor(tr.querySelector('.delete-proveedor').dataset.id));
}

function showProveedorModal(proveedorId = null) {
    const form = document.getElementById('proveedor-form');
    form.reset();
    const idInput = document.getElementById('proveedor-id');

    if (proveedorId) {
        document.getElementById('proveedor-modal-title').textContent = 'Editar Proveedor';
        const proveedor = state.proveedores.find(p => p.id === proveedorId);
        if (proveedor) {
            document.getElementById('proveedor-id-hidden').value = proveedor.id;
            idInput.value = proveedor.id;
            document.getElementById('proveedor-nombre').value = proveedor.nombre;
            document.getElementById('proveedor-rtn').value = proveedor.rtn || '';
            document.getElementById('proveedor-telefono').value = proveedor.telefono || '';
            document.getElementById('proveedor-email').value = proveedor.email || '';
            document.getElementById('proveedor-direccion').value = proveedor.direccion || '';
            idInput.setAttribute('readonly', true);
        }
    } else {
        document.getElementById('proveedor-modal-title').textContent = 'Añadir Proveedor';
        document.getElementById('proveedor-id-hidden').value = '';
        idInput.removeAttribute('readonly');
    }
    state.modals.proveedor.show();
}

async function handleProveedorSubmit(e) {
    e.preventDefault();
    const originalId = document.getElementById('proveedor-id-hidden').value;

    const proveedorData = {
        id: document.getElementById('proveedor-id').value.trim(),
        nombre: document.getElementById('proveedor-nombre').value,
        rtn: document.getElementById('proveedor-rtn').value,
        telefono: document.getElementById('proveedor-telefono').value,
        email: document.getElementById('proveedor-email').value,
        direccion: document.getElementById('proveedor-direccion').value,
    };

    if (!proveedorData.id) {
        showToast('El Código del proveedor no puede estar vacío.', 'error');
        return;
    }

    try {
        const docRef = doc(state.collections.proveedores, proveedorData.id);
        await setDoc(docRef, proveedorData, { merge: true });
        if (originalId && originalId !== proveedorData.id) {
             await deleteDoc(doc(state.collections.proveedores, originalId));
        }
        showToast('Proveedor guardado correctamente', 'success');
    } catch (error) {
        console.error("Error saving proveedor: ", error);
        showToast('Error al guardar el proveedor', 'error');
    }
    state.modals.proveedor.hide();
}

function deleteProveedor(proveedorId) {
    showConfirmation('Eliminar Proveedor', `¿Está seguro de que desea eliminar el proveedor ${proveedorId}?`, async () => {
        try {
            await deleteDoc(doc(state.collections.proveedores, proveedorId));
            showToast('Proveedor eliminado correctamente', 'success');
        } catch (error) {
            console.error("Error deleting proveedor: ", error);
            showToast('Error al eliminar el proveedor', 'error');
        }
    });
}

// --- NEW: Centralized function to update the user modal UI based on role ---
function updateTechnicianModalUIForRole(role) {
    const salaryWrapper = document.getElementById('technician-salary-wrapper');
    const salaryInput = document.getElementById('technician-salary');
    const permissionsWrapper = document.getElementById('technician-permissions-wrapper');
    const jefeMaquinasWrapper = document.getElementById('jefe-maquinas-wrapper');
    const jefePermissionsWrapper = document.getElementById('jefe-permissions-wrapper');

    permissionsWrapper.style.display = role === 'Técnico' ? 'block' : 'none';
    // Use classList.toggle to correctly override Bootstrap's .d-none utility
    jefeMaquinasWrapper.classList.toggle('d-none', role !== 'Jefe de Area');
    jefePermissionsWrapper.classList.toggle('d-none', role !== 'Jefe de Area');

    if (role === 'Operario') {
        salaryWrapper.style.display = 'none';
        salaryInput.removeAttribute('required');
    } else {
        salaryWrapper.style.display = 'block';
        salaryInput.setAttribute('required', true);
    }
}

// --- CRUD Technician/User Functions ---
function renderTechnicians() {
    const tbody = document.getElementById('technician-list');
    tbody.innerHTML = '';
    if (state.technicians.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center">No se encontraron usuarios.</td></tr>';
        return;
    }

    let actionsHTML = 'Sin permisos';
    if(state.currentUser?.role === 'Invitado') {
        actionsHTML = `<span class="text-muted fst-italic">Solo lectura</span>`;
    } else if (state.currentUser?.role === 'Admin') {
         actionsHTML = `
            <button class="btn btn-sm btn-outline-primary edit-tech" data-id="{id}"><i class="fas fa-edit"></i></button> 
            <button class="btn btn-sm btn-outline-danger delete-tech" data-id="{id}" {disabled}><i class="fas fa-trash"></i></button>
        `;
    }
    
    state.technicians.forEach(tech => {
        const tr = document.createElement('tr');
        const isCurrentUser = state.currentUser && state.currentUser.username === tech.username;
        tr.innerHTML = `
            <td>${tech.username}</td>
            <td>${tech.role}</td>
            <td>${tech.salario ? new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' }).format(tech.salario) : 'N/A'}</td>
            <td class="text-center">${actionsHTML.replace(/{id}/g, tech.fb_id).replace('{disabled}', isCurrentUser ? 'disabled' : '')}</td>
        `;
        tbody.appendChild(tr);
    });
    document.querySelectorAll('.edit-tech').forEach(btn => btn.addEventListener('click', () => showTechnicianModal(btn.dataset.id)));
    document.querySelectorAll('.delete-tech:not([disabled])').forEach(btn => btn.addEventListener('click', () => deleteTechnician(btn.dataset.id)));
}

function showTechnicianModal(techId = null) {
    const form = document.getElementById('technician-form');
    form.reset();
    const userInput = document.getElementById('technician-username');
    const passInput = document.getElementById('technician-password');
    const salaryInput = document.getElementById('technician-salary');
    const roleSelect = document.getElementById('technician-role');
    const jefeMaquinasList = document.getElementById('jefe-maquinas-list');
    document.querySelectorAll('.permission-check, .jefe-permission-check').forEach(cb => cb.checked = false);
    jefeMaquinasList.innerHTML = ''; // Clear previous list

    // Populate the machine list for Jefe de Area
    if (state.machines.length > 0) {
        state.machines.forEach(machine => {
            const div = document.createElement('div');
            div.className = 'form-check';
            div.innerHTML = `
                <input class="form-check-input machine-check" type="checkbox" value="${machine.id}" id="machine-check-${machine.id}">
                <label class="form-check-label" for="machine-check-${machine.id}">
                    ${machine.name} (${machine.id})
                </label>
            `;
            jefeMaquinasList.appendChild(div);
        });
    } else {
        jefeMaquinasList.innerHTML = '<p class="text-muted small">No hay máquinas registradas para asignar.</p>';
    }

    if (techId) {
        document.getElementById('technician-modal-title').textContent = 'Editar Usuario';
        const tech = state.technicians.find(t => t.fb_id === techId);
        if (tech) {
            document.getElementById('technician-id-hidden').value = tech.fb_id;
            userInput.value = tech.username;
            salaryInput.value = tech.salario || '';
            roleSelect.value = tech.role;
            userInput.setAttribute('readonly', true);
            passInput.removeAttribute('required');
            passInput.placeholder = "Dejar en blanco para no cambiar";
            if (tech.role === 'Técnico' && tech.permissions) {
                tech.permissions.forEach(perm => {
                    const cb = document.getElementById(`perm-${perm}`);
                    if(cb) cb.checked = true;
                });
            }
            if (tech.role === 'Jefe de Area') {
                if (Array.isArray(tech.managedMachineIds)) {
                    tech.managedMachineIds.forEach(machineId => {
                        const checkbox = document.getElementById(`machine-check-${machineId}`);
                        if (checkbox) {
                            checkbox.checked = true;
                        }
                    });
                }
                if (Array.isArray(tech.permissions)) {
                    tech.permissions.forEach(perm => {
                        const cb = document.getElementById(`jefe-perm-${perm}`);
                        if(cb) cb.checked = true;
                    });
                }
            }
        }
    } else {
        document.getElementById('technician-modal-title').textContent = 'Añadir Usuario';
        document.getElementById('technician-id-hidden').value = '';
        userInput.removeAttribute('readonly');
        passInput.setAttribute('required', true);
        passInput.placeholder = "";
    }

    // Centralized call to update UI based on the selected role
    updateTechnicianModalUIForRole(roleSelect.value);
    
    state.modals.technician.show();
}

async function handleTechnicianSubmit(e) {
    e.preventDefault();
    const techId = document.getElementById('technician-id-hidden').value;
    const password = document.getElementById('technician-password').value;
    const role = document.getElementById('technician-role').value;

    const techData = {
        username: document.getElementById('technician-username').value,
        salario: role === 'Operario' ? 0 : (parseFloat(document.getElementById('technician-salary').value) || 0),
        role: role,
        permissions: [],
        managedMachineIds: []
    };

    if (role === 'Admin') {
        techData.permissions = ['all'];
    } else if (role === 'Jefe de Area') {
        document.querySelectorAll('#jefe-permissions .jefe-permission-check:checked').forEach(cb => {
            techData.permissions.push(cb.value);
        });
        document.querySelectorAll('#jefe-maquinas-list .machine-check:checked').forEach(cb => {
            techData.managedMachineIds.push(cb.value);
        });
    } else if (role === 'Invitado') {
        techData.permissions = ['read-only'];
    } else if (role === 'Operario') {
        techData.permissions = ['solicitudes'];
    } else if (role === 'Técnico') {
        document.querySelectorAll('.permission-check:checked').forEach(cb => {
            techData.permissions.push(cb.value);
        });
    }


    if (password) { techData.password = password; }
    
    try {
        if (techId) {
            await setDoc(doc(state.collections.technicians, techId), techData, { merge: true });
            showToast('Usuario actualizado correctamente', 'success');
        } else {
            await addDoc(state.collections.technicians, techData);
            showToast('Usuario creado correctamente', 'success');
        }
    } catch (error) { 
        console.error("Error saving technician: ", error);
        showToast('Error al guardar el usuario', 'error');
    }
    state.modals.technician.hide();
}

function deleteTechnician(techId) {
     const tech = state.technicians.find(t => t.fb_id === techId);
     if (tech && state.currentUser && tech.username === state.currentUser.username) {
          showToast('No puede eliminar el usuario con el que ha iniciado sesión.', 'error');
          return;
     }
     showConfirmation('Eliminar Usuario', `¿Está seguro de que desea eliminar a ${tech.username}?`, async () => {
        try { 
            await deleteDoc(doc(state.collections.technicians, techId)); 
            showToast('Usuario eliminado correctamente', 'success');
        } catch (error) { 
            console.error("Error deleting technician: ", error);
            showToast('Error al eliminar el usuario', 'error');
        }
    });
}

// --- Solicitud (Request) Functions ---
function renderSolicitudes() {
    const tbody = document.getElementById('solicitud-list');
    tbody.innerHTML = '';
    
    let userSolicitudes = state.solicitudes; 
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        userSolicitudes = state.solicitudes.filter(s => managedIds.has(s.machineId));
    } else if (state.currentUser?.role === 'Operario') {
        userSolicitudes = state.solicitudes.filter(s => s.requester === state.currentUser.username);
    }


    if (userSolicitudes.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center">No ha realizado ninguna solicitud.</td></tr>';
        return;
    }

    userSolicitudes.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    userSolicitudes.forEach(solicitud => {
        const machine = state.machines.find(m => m.id === solicitud.machineId) || { name: 'N/A' };
        const tr = document.createElement('tr');
        
        let statusText = solicitud.status;
        let statusBadgeClass = '';

        if (solicitud.workOrderId) {
            const workOrder = state.workOrders.find(wo => wo.id === solicitud.workOrderId);
            if (workOrder) {
                statusText = workOrder.status;
            } else if (solicitud.status === 'Aprobado') {
                statusText = 'En Planificación';
            }
        }
        
        switch(statusText) {
            case 'Pendiente': 
                statusBadgeClass = 'bg-warning text-dark'; 
                break;
            case 'Aprobado':
            case 'En Planificación':
                statusBadgeClass = 'bg-primary'; 
                statusText = 'En Planificación';
                break;
            case 'En Proceso': 
                statusBadgeClass = 'bg-info text-dark'; 
                break;
            case 'Pausado': 
                statusBadgeClass = 'bg-secondary'; 
                break;
            case 'Completado': 
                statusBadgeClass = 'bg-success'; 
                break;
            case 'Rechazado': 
                statusBadgeClass = 'bg-danger'; 
                break;
            case 'Cancelado':
                statusBadgeClass = 'bg-danger'; 
                break;
            default: 
                statusBadgeClass = 'bg-light text-dark';
        }

        tr.innerHTML = `
            <td>${solicitud.id}</td>
            <td>${machine.name}</td>
            <td>${solicitud.description}</td>
            <td>${new Date(solicitud.createdAt).toLocaleDateString('es-ES')}</td>
            <td><span class="badge ${statusBadgeClass}">${statusText}</span></td>
        `;
        tbody.appendChild(tr);
    });
}

function showSolicitudModal() {
    document.getElementById('solicitud-form').reset();
    const machineSelect = document.getElementById('solicitud-machine');
    machineSelect.innerHTML = '<option value="">Seleccione una máquina...</option>';
    state.machines.forEach(m => machineSelect.innerHTML += `<option value="${m.id}">${m.name}</option>`);
    state.modals.solicitud.show();
}

async function handleSolicitudSubmit(e) {
    e.preventDefault();
    showLoading(true);
    
    const solicitudesCount = (await getDocs(query(state.collections.solicitudes))).size;
    const nextId = `SOL-${(solicitudesCount + 1).toString().padStart(4, '0')}`;

    const solicitudData = {
        id: nextId,
        machineId: document.getElementById('solicitud-machine').value,
        description: document.getElementById('solicitud-description').value,
        requester: state.currentUser.username,
        status: 'Pendiente',
        createdAt: new Date().toISOString()
    };

    if (!solicitudData.machineId || !solicitudData.description) {
        showToast('Todos los campos son obligatorios.', 'error');
        showLoading(false);
        return;
    }

    try {
        await addDoc(state.collections.solicitudes, solicitudData);
        showToast('Solicitud enviada correctamente', 'success');
        state.modals.solicitud.hide();
    } catch(error) {
        console.error("Error submitting solicitud:", error);
        showToast('Error al enviar la solicitud.', 'error');
    } finally {
        showLoading(false);
    }
}

// --- Work Order & Kanban Functions ---
function clearActiveTimers() {
    for (const orderId in state.activeTimers) {
        clearInterval(state.activeTimers[orderId]);
    }
    state.activeTimers = {};
}

function renderActiveWorkView() {
    clearActiveTimers();
    const requestsContainer = document.getElementById('pending-requests');
    const plannedContainer = document.getElementById('planned-tasks');
    const inProgressContainer = document.getElementById('in-progress-tasks');
    const pausedContainer = document.getElementById('paused-tasks');
    requestsContainer.innerHTML = '';
    plannedContainer.innerHTML = '';
    inProgressContainer.innerHTML = '';
    pausedContainer.innerHTML = '';

    let workOrdersToDisplay = state.workOrders;
    let solicitudesToDisplay = state.solicitudes;

    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        workOrdersToDisplay = state.workOrders.filter(wo => managedIds.has(wo.machineId));
        solicitudesToDisplay = state.solicitudes.filter(s => managedIds.has(s.machineId));
    }

    const pendingRequests = solicitudesToDisplay.filter(s => s.status === 'Pendiente');
    const plannedOrders = workOrdersToDisplay.filter(o => o.status === 'Pendiente');
    const inProgressOrders = workOrdersToDisplay.filter(o => o.status === 'En Proceso');
    const pausedOrders = workOrdersToDisplay.filter(o => o.status === 'Pausado');

    if (pendingRequests.length === 0) {
         requestsContainer.innerHTML = '<p class="text-center text-muted">No hay solicitudes pendientes.</p>';
    } else {
        pendingRequests.forEach(req => requestsContainer.appendChild(createSolicitudCard(req)));
    }
    if (plannedOrders.length === 0) {
        plannedContainer.innerHTML = '<p class="text-center text-muted">No hay tareas planificadas.</p>';
    } else {
        plannedOrders.forEach(order => plannedContainer.appendChild(createWorkOrderCard(order)));
    }
    if (inProgressOrders.length === 0) {
        inProgressContainer.innerHTML = '<p class="text-center text-muted">No hay tareas en ejecución.</p>';
    } else {
        inProgressOrders.forEach(order => {
            inProgressContainer.appendChild(createWorkOrderCard(order));
            if (order.workIntervals) {
                const lastInterval = order.workIntervals[order.workIntervals.length - 1];
                if (lastInterval && lastInterval.start && !lastInterval.end) {
                   updateTimerDisplay(order.id, order);
                }
            }
        });
    }
    if (pausedOrders.length === 0) {
        pausedContainer.innerHTML = '<p class="text-center text-muted">No hay tareas pausadas.</p>';
    } else {
        pausedOrders.forEach(order => pausedContainer.appendChild(createWorkOrderCard(order)));
    }
}

function createSolicitudCard(solicitud) {
    const card = document.createElement('div');
    card.className = 'kanban-card';
    const machine = state.machines.find(m => m.id === solicitud.machineId) || { name: 'Desconocido' };
    const isReadOnly = state.currentUser?.role === 'Jefe de Area' || state.currentUser?.role === 'Invitado';

    card.innerHTML = `
        <div class="d-flex justify-content-between align-items-start">
            <div>
                <h6 class="card-title mb-0">${machine.name}</h6>
                <small class="text-muted">Solicitante: ${solicitud.requester}</small>
            </div>
             <span class="badge bg-secondary">${new Date(solicitud.createdAt).toLocaleDateString('es-ES')}</span>
        </div>
        <p class="card-text my-2">${solicitud.description}</p>
        <div class="card-footer bg-transparent p-0 pt-2 border-top">
             ${isReadOnly 
                ? `<p class="text-muted text-center mb-0 small">Solo visualización</p>`
                : `<button class="btn btn-sm btn-success convert-solicitud-btn w-100"><i class="fas fa-check me-2"></i>Crear OT</button>`
             }
        </div>
    `;
    if (!isReadOnly) {
        card.querySelector('.convert-solicitud-btn').addEventListener('click', () => {
            showWorkOrderModal(null, 'Correctivo', solicitud);
        });
    }
    return card;
}

function createWorkOrderCard(order) {
    const card = document.createElement('div');
    card.className = 'kanban-card';
    card.dataset.id = order.id;
    const machine = state.machines.find(m => m.id === order.machineId) || { name: 'Desconocido' };
    const isReadOnly = state.currentUser?.role === 'Invitado' || state.currentUser?.role === 'Jefe de Area';
    
    let footerContent = '';
    
    if (isReadOnly) {
         footerContent = `<p class="text-muted text-center mb-0 small">Solo visualización</p>`;
    } else {
        switch(order.status) {
            case 'Pendiente':
                footerContent = `<button class="btn btn-sm btn-primary start-task-btn w-100"><i class="fas fa-play me-2"></i>Iniciar</button>`;
                break;
            case 'En Proceso':
                footerContent = `
                    <div class="d-flex justify-content-between align-items-center">
                        <span class="timer" id="timer-${order.id}">00:00:00</span>
                        <div class="btn-group">
                            <button class="btn btn-sm btn-warning pause-task-btn" title="Pausar"><i class="fas fa-pause"></i></button>
                            <button class="btn btn-sm btn-success complete-task-btn" title="Completar"><i class="fas fa-check"></i></button>
                        </div>
                    </div>`;
                break;
            case 'Pausado':
                footerContent = `<button class="btn btn-sm btn-info resume-task-btn w-100"><i class="fas fa-play me-2"></i>Reanudar</button>`;
                break;
        }
    }


    card.innerHTML = `
        <div class="d-flex justify-content-between align-items-start">
            <div>
                <h6 class="card-title mb-0" style="cursor: pointer;">${order.id}</h6>
                <small class="text-muted">${machine.name}</small>
            </div>
            <span class="badge bg-${order.type === 'Preventivo' ? 'primary' : 'danger'}">${order.type}</span>
        </div>
        <p class="card-text my-2">${order.description}</p>
        <div class="card-footer bg-transparent p-0 pt-2 border-top">
            ${footerContent}
        </div>
    `;
    
    card.querySelector('.card-title').addEventListener('click', () => showWorkOrderModal(order.id));
    if (!isReadOnly) {
        card.querySelector('.start-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'En Proceso'));
        card.querySelector('.pause-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'Pausado'));
        card.querySelector('.resume-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'En Proceso'));
        card.querySelector('.complete-task-btn')?.addEventListener('click', () => {
             showConfirmation(
                'Completar Orden de Trabajo',
                `¿Está seguro que desea completar la orden ${order.id}?`,
                () => handleWorkOrderAction(order.id, 'Completado')
            );
        });
    }

    return card;
}

function getTotalWorkDurationMs(order) {
    let totalMs = 0;
    if (order.workIntervals && Array.isArray(order.workIntervals)) {
         order.workIntervals.forEach(interval => {
            if (interval.start && interval.end) {
                const start = new Date(interval.start);
                const end = new Date(interval.end);
                if (!isNaN(start.getTime()) && !isNaN(end.getTime())) {
                    totalMs += end - start;
                }
            }
        });
    }
    return totalMs;
}

function getActiveWorkDurationMs(order) {
    let totalMs = getTotalWorkDurationMs(order);
    if (order.status === 'En Proceso' && order.workIntervals) {
         const lastInterval = order.workIntervals[order.workIntervals.length - 1];
         if (lastInterval && lastInterval.start && !lastInterval.end) {
             totalMs += new Date() - new Date(lastInterval.start);
         }
    }
    return totalMs;
}


function updateTimerDisplay(orderId, order) {
    const timerEl = document.getElementById(`timer-${orderId}`);
    if (!timerEl) return;

    if(state.activeTimers[orderId]) clearInterval(state.activeTimers[orderId]);

    const intervalId = setInterval(() => {
        const totalMs = getActiveWorkDurationMs(order);
        if (totalMs < 0) {
             timerEl.textContent = '00:00:00';
             return;
        }
        const hours = Math.floor(totalMs / (1000 * 60 * 60));
        const minutes = Math.floor((totalMs % (1000 * 60 * 60)) / (1000 * 60));
        const seconds = Math.floor((totalMs % (1000 * 60)) / 1000);
        timerEl.textContent = `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    }, 1000);

    state.activeTimers[orderId] = intervalId;
}

// --- Assigned Orders Functions (for Technicians) ---
function renderAssignedOrders() {
    const container = document.getElementById('assigned-orders-container');
    if (!container) return;
    container.innerHTML = '';

    if (!state.currentUser || (state.currentUser.role !== 'Técnico' && state.currentUser.role !== 'Admin')) {
        container.innerHTML = '<div class="alert alert-warning">No tiene permisos para ver esta sección.</div>';
        return;
    }

    const assignedOrders = state.workOrders.filter(order =>
        order.technicians && order.technicians.includes(state.currentUser.username) &&
        ['Pendiente', 'En Proceso', 'Pausado'].includes(order.status)
    );

    if (assignedOrders.length === 0) {
        container.innerHTML = '<p class="text-center text-muted mt-4">No tiene órdenes de trabajo asignadas activas.</p>';
        return;
    }

    assignedOrders.sort((a, b) => new Date(a.date) - new Date(b.date));

    assignedOrders.forEach(order => {
        container.appendChild(createAssignedOrderCard(order));
    });
}

function createAssignedOrderCard(order) {
    const card = document.createElement('div');
    card.className = 'card kanban-card';
    const machine = state.machines.find(m => m.id === order.machineId) || { name: 'Desconocido' };
    const isLead = state.currentUser.username === order.leadTechnician;

    let actionButtonsHTML = '';
    if(isLead) {
        let statusButtons = '';
        switch (order.status) {
            case 'Pendiente':
                statusButtons = `<button class="btn btn-primary start-task-btn"><i class="fas fa-play me-2"></i>Iniciar</button>`;
                break;
            case 'En Proceso':
                statusButtons = `
                    <button class="btn btn-warning pause-task-btn"><i class="fas fa-pause me-2"></i>Pausar</button>
                    <button class="btn btn-success complete-task-btn ms-2"><i class="fas fa-check me-2"></i>Finalizar</button>
                `;
                break;
            case 'Pausado':
                statusButtons = `<button class="btn btn-info resume-task-btn"><i class="fas fa-play me-2"></i>Reanudar</button>`;
                break;
        }
        actionButtonsHTML = `
            <div class="btn-group">
                ${statusButtons}
            </div>
            <div class="btn-group ms-2" role="group">
                <button class="btn btn-outline-secondary edit-details-btn" title="Editar Detalles y Falla"><i class="fas fa-edit"></i></button>
                <button class="btn btn-outline-secondary manage-parts-btn" title="Gestionar Repuestos"><i class="fas fa-box-open"></i></button>
            </div>
        `;
    } else {
         actionButtonsHTML = `<button class="btn btn-sm btn-outline-secondary" disabled title="Solo el técnico responsable puede cambiar el estado.">Ver Detalles</button>`;
    }

    card.innerHTML = `
        <div class="card-body">
            <div class="d-flex justify-content-between">
                <div>
                    <h5 class="card-title" style="cursor: pointer;">${order.id}</h5>
                    <h6 class="card-subtitle mb-2 text-muted">${machine.name}</h6>
                </div>
                <span class="badge bg-${order.status === 'En Proceso' ? 'info text-dark' : (order.status === 'Pausado' ? 'secondary' : 'warning text-dark')}">${order.status}</span>
            </div>
            <p class="card-text"><b>Responsable:</b> ${order.leadTechnician || 'N/A'}</p>
            <p class="card-text">${order.description}</p>
            <div class="d-flex justify-content-between align-items-center mt-3 pt-3 border-top">
                <small class="text-muted">Fecha Límite: ${new Date(order.date).toLocaleDateString('es-ES')}</small>
                <div>${actionButtonsHTML}</div>
            </div>
        </div>
    `;

    if (isLead) {
        card.querySelector('.start-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'En Proceso'));
        card.querySelector('.pause-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'Pausado'));
        card.querySelector('.resume-task-btn')?.addEventListener('click', () => handleWorkOrderAction(order.id, 'En Proceso'));
        card.querySelector('.complete-task-btn')?.addEventListener('click', () => {
            showConfirmation(
                'Finalizar Orden de Trabajo',
                `¿Está seguro que desea marcar la orden ${order.id} como finalizada?`,
                () => handleWorkOrderAction(order.id, 'Completado')
            );
        });
        card.querySelector('.manage-parts-btn')?.addEventListener('click', () => showManagePartsModal(order.id));
        card.querySelector('.edit-details-btn')?.addEventListener('click', () => showWorkOrderModal(order.id));
    }
    
    card.querySelector('.card-title').addEventListener('click', () => showWorkOrderModal(order.id));
    if (!isLead) {
        card.querySelector('button[disabled]')?.addEventListener('click', () => showWorkOrderModal(order.id));
    }

    return card;
}

function showManagePartsModal(orderId) {
    const order = state.workOrders.find(o => o.id === orderId);
    if (!order) {
        showToast('Orden de trabajo no encontrada.', 'error');
        return;
    }

    document.getElementById('manage-parts-modal-title').textContent = `Gestionar Repuestos para OT: ${orderId}`;
    document.getElementById('manage-parts-wo-id').value = orderId;

    const partsListContainer = document.getElementById('current-parts-list');
    partsListContainer.innerHTML = '';
    
    const currentParts = order.partsUsed || [];
    if (currentParts.length > 0) {
        currentParts.forEach(partUsage => {
            renderPartInManageModal(partUsage.partId, partUsage.quantity);
        });
    } else {
        partsListContainer.innerHTML = '<p class="text-muted list-group-item">No hay repuestos asignados a esta orden.</p>';
    }

    const partSelect = document.getElementById('manage-part-select');
    partSelect.innerHTML = '<option value="">Seleccione un repuesto...</option>';
    const relevantParts = state.parts.filter(p => 
        (p.machineIds && p.machineIds.includes(order.machineId)) || 
        (p.machineId === order.machineId)
    );
    relevantParts.forEach(p => {
        partSelect.innerHTML += `<option value="${p.id}">${p.description} (Stock: ${p.stock})</option>`;
    });

    state.modals.manageParts.show();
}

function renderPartInManageModal(partId, quantity) {
    const part = state.parts.find(p => p.id === partId);
    if (!part) return;

    const listContainer = document.getElementById('current-parts-list');
    
    const placeholder = listContainer.querySelector('p');
    if (placeholder) placeholder.remove();

    let partRow = listContainer.querySelector(`[data-part-id="${partId}"]`);
    if(partRow) {
        partRow.dataset.quantity = quantity;
        partRow.querySelector('.part-quantity-badge').textContent = `Cant: ${quantity}`;
    } else {
        partRow = document.createElement('div');
        partRow.className = 'list-group-item d-flex justify-content-between align-items-center';
        partRow.dataset.partId = partId;
        partRow.dataset.quantity = quantity;

        partRow.innerHTML = `
            <span>${part.description}</span>
            <div>
                <span class="badge bg-secondary me-2 part-quantity-badge">Cant: ${quantity}</span>
                <button type="button" class="btn btn-sm btn-outline-danger remove-managed-part-btn"><i class="fas fa-times"></i></button>
            </div>
        `;
        listContainer.appendChild(partRow);
        
        partRow.querySelector('.remove-managed-part-btn').addEventListener('click', () => {
            partRow.remove();
            if (listContainer.children.length === 0) {
                 listContainer.innerHTML = '<p class="text-muted list-group-item">No hay repuestos asignados a esta orden.</p>';
            }
        });
    }
}

function handleAddOrUpdatePartInModal() {
    const partSelect = document.getElementById('manage-part-select');
    const qtyInput = document.getElementById('manage-part-quantity');
    const selectedPartId = partSelect.value;
    const selectedQty = parseInt(qtyInput.value);

    if (!selectedPartId) {
        showToast('Seleccione un repuesto.', 'warning');
        return;
    }
    if (isNaN(selectedQty) || selectedQty < 1) {
        showToast('La cantidad debe ser un número positivo.', 'warning');
        return;
    }
    renderPartInManageModal(selectedPartId, selectedQty);
    partSelect.value = '';
    qtyInput.value = 1;
}

async function handleSaveManagedParts() {
    const orderId = document.getElementById('manage-parts-wo-id').value;
    if (!orderId) return;

    showLoading(true);

    const originalOrder = state.workOrders.find(o => o.id === orderId);
    if (!originalOrder) {
        showToast('Orden no encontrada.', 'error');
        showLoading(false);
        return;
    }
    
    const originalPartsMap = new Map((originalOrder.partsUsed || []).map(p => [p.partId, p.quantity]));
    
    const newPartsList = [];
    const newPartsMap = new Map();
    document.querySelectorAll('#current-parts-list .list-group-item[data-part-id]').forEach(item => {
        const partId = item.dataset.partId;
        const quantity = parseInt(item.dataset.quantity);
        newPartsList.push({ partId, quantity });
        newPartsMap.set(partId, quantity);
    });

    const stockDeltas = new Map();
    newPartsMap.forEach((newQty, partId) => {
        const oldQty = originalPartsMap.get(partId) || 0;
        const delta = newQty - oldQty;
        if (delta !== 0) {
            stockDeltas.set(partId, (stockDeltas.get(partId) || 0) - delta); // Subtract delta from stock
        }
    });
    
    originalPartsMap.forEach((oldQty, partId) => {
        if (!newPartsMap.has(partId)) {
            stockDeltas.set(partId, (stockDeltas.get(partId) || 0) + oldQty); // Add back to stock
        }
    });

    try {
        for (const [partId, delta] of stockDeltas.entries()) {
            const partDocRef = doc(state.collections.parts, partId);
            const partDoc = await getDoc(partDocRef);
            if (partDoc.exists()) {
                const currentStock = partDoc.data().stock;
                const newStock = currentStock + delta;

                if (newStock < 0) {
                    throw new Error(`Stock insuficiente para ${partId}. Stock: ${currentStock}, Necesita: ${-delta}.`);
                }
                await updateDoc(partDocRef, { stock: newStock });
            }
        }
        
        const orderDocRef = doc(state.collections.workOrders, originalOrder.fb_id);
        await updateDoc(orderDocRef, { partsUsed: newPartsList });
        
        showToast('Repuestos actualizados correctamente.', 'success');
        state.modals.manageParts.hide();

    } catch (error) {
        console.error("Error saving managed parts:", error);
        showToast(error.message || 'Error al guardar los cambios en los repuestos.', 'error');
    } finally {
        showLoading(false);
    }
}


// --- Planner/Calendar Functions ---
function renderCalendar() {
    const grid = document.getElementById('calendar-grid');
    const monthYearEl = document.getElementById('calendar-month-year');
    grid.innerHTML = '';

    const date = state.calendarDate;
    const year = date.getFullYear();
    const month = date.getMonth();

    monthYearEl.textContent = date.toLocaleDateString('es-ES', { month: 'long', year: 'numeric' });

    const firstDay = new Date(year, month, 1).getDay();
    const daysInMonth = new Date(year, month + 1, 0).getDate();

    let workOrdersToDisplay = state.workOrders;
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        workOrdersToDisplay = state.workOrders.filter(wo => managedIds.has(wo.machineId));
    }

    for (let i = 0; i < firstDay; i++) {
        grid.innerHTML += '<div class="calendar-day not-month"></div>';
    }

    for (let day = 1; day <= daysInMonth; day++) {
        const dayEl = document.createElement('div');
        dayEl.className = 'calendar-day';
        dayEl.innerHTML = `<div class="day-number">${day}</div>`;
        
        const currentDateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        
        const tasksForDay = workOrdersToDisplay.filter(wo => wo.date === currentDateStr);
        
        tasksForDay.forEach(task => {
            const taskEl = document.createElement('div');
            taskEl.className = `calendar-task task-${task.type}`;
            taskEl.textContent = task.id;
            taskEl.dataset.id = task.id;
            
            taskEl.setAttribute('data-bs-toggle', 'popover');
            taskEl.setAttribute('data-bs-trigger', 'hover focus');
            taskEl.setAttribute('data-bs-title', `Detalles de OT: ${task.id}`);
            taskEl.setAttribute('data-bs-content', `<b>Máquina:</b> ${task.machineId}<br><b>Tarea:</b> ${task.description}`);
            taskEl.setAttribute('data-bs-html', 'true');

            taskEl.addEventListener('click', (e) => {
                e.stopPropagation();
                showWorkOrderModal(task.id);
            });
            dayEl.appendChild(taskEl);
        });

        grid.appendChild(dayEl);
    }

    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
}

function changeMonth(offset) {
    state.calendarDate.setMonth(state.calendarDate.getMonth() + offset);
    renderCalendar();
}

// --- Comprehensive Work Order Modal Functions ---
async function saveWorkOrder(orderId, updates = {}) {
    showLoading(true);
    try {
        const idRegex = /^MA-\d{2}-\d{4}$/;
        if (!idRegex.test(orderId)) {
            showToast('Formato de ID incorrecto. Debe ser MA-AA-NNNN (ej. MA-25-0001).', 'error');
            showLoading(false);
            return;
        }

        const isNew = !document.getElementById('work-order-id-hidden').value;
        if (isNew && state.workOrders.some(wo => wo.id === orderId)) {
            showToast('El ID de la orden de trabajo ya existe.', 'error');
            showLoading(false);
            return;
        }

        const partsUsed = [];
        document.querySelectorAll('#wo-parts-list > div').forEach(row => {
            partsUsed.push({
                partId: row.dataset.partId,
                quantity: parseInt(row.dataset.quantity)
            });
        });

        const leadTechnician = document.getElementById('wo-lead-technician-select').value;
        const supportTechnicians = [];
        document.querySelectorAll('#wo-support-technicians-list .assigned-technician-badge').forEach(badge => {
            supportTechnicians.push(badge.dataset.username);
        });

        const startDate = document.getElementById('wo-start-date').value;
        const startTime = document.getElementById('wo-start-time').value;
        const endDate = document.getElementById('wo-end-date').value;
        const endTime = document.getElementById('wo-end-time').value;

        let startTimeISO = null;
        if (startDate && startTime) {
            startTimeISO = new Date(`${startDate}T${startTime}`).toISOString();
        } else if (startDate) {
            startTimeISO = new Date(startDate).toISOString();
        }

        let endTimeISO = null;
        if (endDate && endTime) {
            endTimeISO = new Date(`${endDate}T${endTime}`).toISOString();
        } else if (endDate) {
            endTimeISO = new Date(endDate).toISOString();
        }

        if (startTimeISO && endTimeISO && new Date(endTimeISO) <= new Date(startTimeISO)) {
            showToast('La fecha/hora final debe ser posterior a la fecha/hora de inicio.', 'error');
            showLoading(false);
            return;
        }

        const formData = {
            id: orderId,
            machineId: document.getElementById('wo-machine').value,
            description: document.getElementById('wo-description').value,
            status: document.getElementById('wo-status').value,
            type: document.getElementById('wo-type').value,
            date: startDate, // Main date for calendar
            startTime: startTimeISO,
            endTime: endTimeISO,
            requester: document.getElementById('wo-requester').value,
            leadTechnician: leadTechnician,
            supportTechnicians: supportTechnicians,
            technicians: [...new Set([leadTechnician, ...supportTechnicians].filter(Boolean))],
            partsUsed: partsUsed,
            failureType: document.getElementById('wo-type').value === 'Correctivo' 
                ? document.getElementById('wo-failure-type').value 
                : null,
            maintenanceType: document.getElementById('wo-type').value === 'Preventivo'
                ? document.getElementById('wo-maintenance-type').value
                : null,
        };

        if (formData.type === 'Correctivo' && !formData.failureType) {
            showToast('Para órdenes correctivas, es obligatorio seleccionar un tipo de falla.', 'error');
            showLoading(false);
            return;
        }

        if (formData.type === 'Preventivo' && !formData.maintenanceType) {
            showToast('Para órdenes preventivas, es obligatorio seleccionar un tipo de mantenimiento.', 'error');
            showLoading(false);
            return;
        }

        if (!formData.leadTechnician && formData.status !== 'Cancelado') {
            showToast('Debe asignar un técnico responsable para guardar la orden.', 'error');
            showLoading(false);
            return;
        }
        
        const existingOrder = isNew ? {} : state.workOrders.find(wo => wo.id === orderId) || {};
        const orderData = { ...existingOrder, ...formData, ...updates };
        
        const oldStatus = existingOrder.status;
        const newStatus = orderData.status;
        let workIntervals = existingOrder.workIntervals ? JSON.parse(JSON.stringify(existingOrder.workIntervals)) : [];

        if (newStatus === 'Completado' && orderData.startTime && orderData.endTime) {
            const start = new Date(orderData.startTime);
            const end = new Date(orderData.endTime);
            if (end > start) {
                orderData.workIntervals = [{ start: start.toISOString(), end: end.toISOString() }];
            }
        } else if (newStatus !== oldStatus) {
            if (newStatus === 'En Proceso' && oldStatus !== 'En Proceso') {
                let intervalStart = new Date();
                if (!workIntervals.some(i => !i.end) && orderData.startTime) {
                    const manualStart = new Date(orderData.startTime);
                    if (manualStart <= intervalStart) {
                        intervalStart = manualStart;
                    }
                }
                workIntervals.push({ start: intervalStart.toISOString() });
            } else if (['Pausado', 'Completado'].includes(newStatus) && oldStatus === 'En Proceso') {
                const lastInterval = workIntervals.find(i => !i.end);
                if (lastInterval) {
                    lastInterval.end = new Date().toISOString();
                }
            }
            orderData.workIntervals = workIntervals;
        }

        if (newStatus === 'Completado' && !orderData.endTime) {
            const lastInterval = orderData.workIntervals[orderData.workIntervals.length - 1];
            orderData.endTime = lastInterval?.end || new Date().toISOString();
        }

        if (isNew) {
            orderData.createdAt = new Date().toISOString();
        }
        
        const isCompletingNow = newStatus === 'Completado' && oldStatus !== 'Completado';

        if (isCompletingNow && orderData.partsUsed.length > 0) {
            for (const partUsage of orderData.partsUsed) {
                if (partUsage.quantity > 0) {
                    const partToUpdate = state.parts.find(p => p.id === partUsage.partId);
                    if (partToUpdate) {
                        const partDocRef = doc(state.collections.parts, partToUpdate.fb_id);
                        const partDoc = await getDoc(partDocRef);
                        if (partDoc.exists()) {
                            const currentStock = partDoc.data().stock;
                            const newStock = currentStock - partUsage.quantity;
                            if (newStock < 0) {
                                throw new Error(`Stock insuficiente para ${partUsage.partId}. Stock: ${currentStock}, Necesita: ${partUsage.quantity}.`);
                            }
                            await updateDoc(partDocRef, { stock: newStock });
                        }
                    }
                }
            }
        }

        const docRef = doc(state.collections.workOrders, orderId);
        await setDoc(docRef, orderData, { merge: true });
        
        const sourceSolicitudId = document.getElementById('source-solicitud-id-hidden').value;
        if (sourceSolicitudId) {
            const solicitud = state.solicitudes.find(s => s.fb_id === sourceSolicitudId);
            if(solicitud && solicitud.status === 'Pendiente') {
               await updateDoc(doc(state.collections.solicitudes, sourceSolicitudId), { 
                   status: 'Aprobado',
                   workOrderId: orderId
                });
            }
        }

        showToast('Orden de trabajo guardada.', 'success');
        state.modals.workOrder.hide();
    } catch (error) {
        console.error("Error saving work order:", error);
        showToast('Error al guardar la orden de trabajo.', 'error');
    } finally {
        showLoading(false);
    }
}

function handleWorkOrderSaveClick() {
    const orderId = document.getElementById('wo-id').value.trim();
    if (orderId) saveWorkOrder(orderId);
}

async function handleWorkOrderAction(orderId, newStatus) {
    if (!orderId) return;
    
    const order = state.workOrders.find(wo => wo.id === orderId);
    if (!order) return;
    const orderRef = doc(state.collections.workOrders, order.fb_id);

    if (state.currentUser.role === 'Técnico' && order.leadTechnician !== state.currentUser.username) {
        showToast('Solo el técnico responsable puede cambiar el estado de la orden.', 'error');
        return;
    }

    showLoading(true);

    try {
        const updates = { status: newStatus };
        let workIntervals = order.workIntervals ? [...order.workIntervals] : [];
        const now = new Date(); // Use a single, consistent timestamp for the action

        if (newStatus === 'En Proceso') {
            if (!order.leadTechnician) {
                showToast('Debe asignar un técnico responsable antes de iniciar.', 'warning');
                showWorkOrderModal(orderId);
                showLoading(false);
                return;
            }
            
            // If starting for the very first time, also update the main start date/time
            if (order.status === 'Pendiente') {
                updates.startTime = now.toISOString();
                updates.date = now.toISOString().split('T')[0];
            }

            workIntervals.push({ start: now.toISOString() });
        } else if (newStatus === 'Pausado' || newStatus === 'Completado') {
            const lastInterval = workIntervals[workIntervals.length - 1];
            if (lastInterval && lastInterval.start && !lastInterval.end) {
                lastInterval.end = now.toISOString();
            }
            if (newStatus === 'Completado' && !order.endTime) {
                updates.endTime = now.toISOString();
            }
        }
        updates.workIntervals = workIntervals;
        
        // Stock deduction logic for quick buttons
        if (newStatus === 'Completado' && order.status !== 'Completado' && order.partsUsed && order.partsUsed.length > 0) {
            for (const partUsage of order.partsUsed) {
                const partToUpdate = state.parts.find(p => p.id === partUsage.partId);
                if (partToUpdate) {
                    const partDocRef = doc(state.collections.parts, partToUpdate.fb_id);
                    const partDoc = await getDoc(partDocRef);
                    if (partDoc.exists()) {
                        const currentStock = partDoc.data().stock;
                        const newStock = currentStock - partUsage.quantity;
                         if (newStock < 0) {
                            throw new Error(`Stock insuficiente para ${partUsage.partId}. No se puede completar la orden.`);
                        }
                        await updateDoc(partDocRef, { stock: newStock });
                    }
                }
            }
        }

        await updateDoc(orderRef, updates);
        showToast(`Orden de trabajo actualizada a: ${newStatus}`, 'success');
        if (state.modals.workOrder._isShown) {
            state.modals.workOrder.hide();
        }
    } catch (error) {
        console.error(`Error updating work order to ${newStatus}:`, error);
        showToast('Error al actualizar la orden de trabajo.', 'error');
    } finally {
        showLoading(false);
    }
}

function showWorkOrderModal(orderId = null, type = 'Preventivo', sourceSolicitud = null) {
    const form = document.getElementById('work-order-form');
    form.reset();
    document.getElementById('wo-parts-list').innerHTML = '';
    document.getElementById('wo-support-technicians-list').innerHTML = '';
    document.getElementById('source-solicitud-id-hidden').value = '';
    document.getElementById('wo-start-date').value = '';
    document.getElementById('wo-start-time').value = '';
    document.getElementById('wo-end-date').value = '';
    document.getElementById('wo-end-time').value = '';

    const idInput = document.getElementById('wo-id');
    const saveBtn = document.getElementById('wo-save-btn');
    const addPartBtn = document.getElementById('wo-add-part-btn');
    const addSupportTechBtn = document.getElementById('wo-add-support-technician-btn');
    
    const allFields = document.querySelectorAll('#work-order-form input, #work-order-form select, #work-order-form textarea');
    allFields.forEach(field => field.disabled = false);
    saveBtn.style.display = 'inline-block';
    addPartBtn.disabled = false;
    addSupportTechBtn.disabled = false;
    
    const leadTechSelect = document.getElementById('wo-lead-technician-select');
    const supportTechSelect = document.getElementById('wo-support-technician-select');
    leadTechSelect.innerHTML = '<option value="">Seleccione responsable...</option>';
    supportTechSelect.innerHTML = '<option value="">Seleccione apoyo...</option>';
    state.technicians.forEach(t => {
        if(t.role !== 'Invitado' && t.role !== 'Operario') {
            leadTechSelect.innerHTML += `<option value="${t.username}">${t.username}</option>`;
            supportTechSelect.innerHTML += `<option value="${t.username}">${t.username}</option>`;
        }
    });

    const partSelect = document.getElementById('wo-part-select');
    partSelect.innerHTML = '<option value="">Seleccione repuesto...</option>';
    state.parts.forEach(p => partSelect.innerHTML += `<option value="${p.id}">${p.description} (Stock: ${p.stock})</option>`);
    
    const statusSelect = document.getElementById('wo-status');
    statusSelect.innerHTML = ['Pendiente', 'En Proceso', 'Pausado', 'Completado', 'Cancelado'].map(s => `<option>${s}</option>`).join('');
    
    let order = null;
    if (orderId) {
        order = state.workOrders.find(o => o.id === orderId);
        document.getElementById('work-order-modal-title').textContent = `Detalles de OT: ${orderId}`;
        idInput.readOnly = true;
        if (order) {
            document.getElementById('work-order-id-hidden').value = order.id;
            idInput.value = order.id;
            document.getElementById('wo-machine').value = order.machineId;
            document.getElementById('wo-description').value = order.description;
            document.getElementById('wo-status').value = order.status;
            document.getElementById('wo-type').value = order.type;
            
            if (order.startTime) {
                const startDate = new Date(order.startTime);
                document.getElementById('wo-start-date').value = startDate.toISOString().split('T')[0];
                document.getElementById('wo-start-time').value = startDate.toTimeString().split(' ')[0].substring(0, 5);
            } else if (order.date) { // For backward compatibility
                document.getElementById('wo-start-date').value = order.date;
            }

            if (order.endTime) {
                const endDate = new Date(order.endTime);
                document.getElementById('wo-end-date').value = endDate.toISOString().split('T')[0];
                document.getElementById('wo-end-time').value = endDate.toTimeString().split(' ')[0].substring(0, 5);
            }
            
            document.getElementById('wo-requester').value = order.requester;
            document.getElementById('wo-failure-type').value = order.failureType || '';
            document.getElementById('wo-maintenance-type').value = order.maintenanceType || '';
            
            const lead = order.leadTechnician || (order.technicians && order.technicians[0]) || '';
            const support = order.supportTechnicians || (order.technicians && order.technicians.slice(1)) || [];
            
            leadTechSelect.value = lead;
            support.forEach(techUsername => addSupportTechnicianToWorkOrderUI(techUsername));

            if(order.partsUsed) {
                order.partsUsed.forEach(part => addPartToWorkOrder(part.partId, part.quantity, true));
            }
        }
    } else {
        document.getElementById('work-order-modal-title').textContent = sourceSolicitud ? `Nueva OT desde Solicitud` : `Nueva Orden ${type}`;
        const newId = generateNextWorkOrderId();
        document.getElementById('work-order-id-hidden').value = ''; 
        idInput.value = newId;
        idInput.readOnly = false;
        document.getElementById('wo-type').value = type;
        document.getElementById('wo-start-date').value = new Date().toISOString().split('T')[0];
        document.getElementById('wo-requester').value = sourceSolicitud ? sourceSolicitud.requester : state.currentUser.username;
        document.getElementById('wo-status').value = 'Pendiente';

        if (sourceSolicitud) {
            document.getElementById('source-solicitud-id-hidden').value = sourceSolicitud.fb_id;
            document.getElementById('wo-machine').value = sourceSolicitud.machineId;
            document.getElementById('wo-description').value = sourceSolicitud.description;
        }
    }
    
    const isLeadTechnician = order ? state.currentUser.username === order.leadTechnician : false;
    const canControl = state.currentUser.role === 'Admin' || isLeadTechnician || !orderId;
    
    if (!canControl || state.currentUser.role === 'Invitado' || order?.status === 'Completado' || order?.status === 'Cancelado') {
        allFields.forEach(field => field.disabled = true);
        saveBtn.style.display = 'none';
        addPartBtn.disabled = true;
        addSupportTechBtn.disabled = true;
        document.querySelectorAll('#wo-parts-list button, #wo-support-technicians-list button').forEach(btn => btn.disabled = true);
    }

    handleWorkOrderTypeChange();
    updateWorkOrderModalButtons();
    state.modals.workOrder.show();
}

function handleWorkOrderTypeChange() {
    const type = document.getElementById('wo-type').value;
    const failureGroup = document.getElementById('wo-failure-type-group');
    const maintenanceGroup = document.getElementById('wo-maintenance-type-group');
    failureGroup.classList.toggle('d-none', type !== 'Correctivo');
    maintenanceGroup.classList.toggle('d-none', type !== 'Preventivo');
}

function generateNextWorkOrderId() {
    const currentYear = new Date().getFullYear().toString().slice(-2);
    const prefix = `MA-${currentYear}-`;
    
    const relevantOrders = state.workOrders
        .filter(wo => wo.id.startsWith(prefix))
        .map(wo => parseInt(wo.id.split('-')[2]))
        .filter(num => !isNaN(num));

    const maxNumber = relevantOrders.length > 0 ? Math.max(...relevantOrders) : 0;
    const nextNumber = maxNumber + 1;
    const nextSequence = nextNumber.toString().padStart(4, '0');
    
    return prefix + nextSequence;
}

function addPartToWorkOrder(partId, quantity, isInitialLoad = false) {
    const partSelect = document.getElementById('wo-part-select');
    const qtyInput = document.getElementById('wo-part-quantity');

    const selectedPartId = isInitialLoad ? partId : partSelect.value;
    const selectedQty = isInitialLoad ? quantity : parseInt(qtyInput.value);

    if (!selectedPartId || isNaN(selectedQty) || selectedQty < 1) return;

    const part = state.parts.find(p => p.id === selectedPartId);
    if (!part) return;

    const list = document.getElementById('wo-parts-list');
    
    if (!isInitialLoad) {
        const existingRow = list.querySelector(`[data-part-id="${selectedPartId}"]`);
        if (existingRow) {
            showToast('Este repuesto ya ha sido añadido.', 'warning');
            return;
        }
    }

    const partRow = document.createElement('div');
    partRow.className = 'd-flex justify-content-between align-items-center bg-white border rounded p-2 mb-1';
    partRow.dataset.partId = part.id;
    partRow.dataset.quantity = selectedQty;
    partRow.innerHTML = `
        <span>${part.description}</span>
        <div>
            <span class="badge bg-secondary me-2">Cant: ${selectedQty}</span>
            <button type="button" class="btn btn-sm btn-outline-danger" onclick="this.parentElement.parentElement.remove()"><i class="fas fa-times"></i></button>
        </div>
    `;
    list.appendChild(partRow);

    if (!isInitialLoad) {
        partSelect.value = '';
        qtyInput.value = 1;
    }
}

function updateWorkOrderModalButtons() {
    const orderId = document.getElementById('work-order-id-hidden').value;
    if (!orderId) {
        ['wo-start-btn', 'wo-pause-btn', 'wo-resume-btn', 'wo-complete-btn'].forEach(id => document.getElementById(id).style.display = 'none');
        return;
    }

    const order = state.workOrders.find(wo => wo.id === orderId);
    if (!order) return;

    const status = document.getElementById('wo-status').value;
    const startBtn = document.getElementById('wo-start-btn');
    const pauseBtn = document.getElementById('wo-pause-btn');
    const resumeBtn = document.getElementById('wo-resume-btn');
    const completeBtn = document.getElementById('wo-complete-btn');

    startBtn.style.display = 'none';
    pauseBtn.style.display = 'none';
    resumeBtn.style.display = 'none';
    completeBtn.style.display = 'none';

    const isLeadTechnician = state.currentUser.username === order.leadTechnician;
    const canControl = state.currentUser.role === 'Admin' || isLeadTechnician;

    if (!canControl) return;

    switch (status) {
        case 'Pendiente':
            startBtn.style.display = 'inline-block';
            break;
        case 'En Proceso':
            pauseBtn.style.display = 'inline-block';
            completeBtn.style.display = 'inline-block';
            break;
        case 'Pausado':
            resumeBtn.style.display = 'inline-block';
            completeBtn.style.display = 'inline-block';
            break;
    }
}

function handleAddSupportTechnicianClick() {
    const techSelect = document.getElementById('wo-support-technician-select');
    const username = techSelect.value;
    if (!username) {
        showToast('Por favor, seleccione un técnico de apoyo.', 'warning');
        return;
    }
    const leadUsername = document.getElementById('wo-lead-technician-select').value;
    if (username === leadUsername) {
        showToast('Este técnico ya es el responsable de la orden.', 'warning');
        return;
    }
    addSupportTechnicianToWorkOrderUI(username);
    techSelect.value = '';
}

function addSupportTechnicianToWorkOrderUI(username) {
    const listContainer = document.getElementById('wo-support-technicians-list');
    const existing = listContainer.querySelector(`[data-username="${username}"]`);
    if (existing) {
        showToast(`El técnico ${username} ya está en la lista de apoyo.`, 'warning');
        return;
    }

    const badge = document.createElement('span');
    badge.className = 'assigned-technician-badge';
    badge.dataset.username = username;
    badge.innerHTML = `
        ${username}
        <button type="button" class="btn-remove-tech"><i class="fas fa-times"></i></button>
    `;
    
    badge.querySelector('.btn-remove-tech').addEventListener('click', (e) => {
        e.currentTarget.closest('.assigned-technician-badge').remove();
    });

    listContainer.appendChild(badge);
}

function handleLeadTechnicianChange(e) {
    const leadUsername = e.target.value;
    if (!leadUsername) return;
    
    const supportList = document.getElementById('wo-support-technicians-list');
    const existingSupportBadge = supportList.querySelector(`[data-username="${leadUsername}"]`);
    if (existingSupportBadge) {
        existingSupportBadge.remove();
        showToast(`${leadUsername} ha sido movido a técnico responsable.`, 'info');
    }
}

// --- Reports Functions ---
function generateGeneralReport() {
    showLoading(true);
    setTimeout(() => {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();

        doc.text("Reporte General de Inventario", 14, 20);
        doc.setFontSize(10);
        doc.text(`Fecha de generación: ${new Date().toLocaleDateString('es-ES')}`, 14, 26);
        
        doc.autoTable({
            startY: 35,
            head: [['ID', 'Nombre', 'Ubicación']],
            body: state.machines.map(m => [m.id, m.name, m.location]),
            headStyles: { fillColor: [44, 62, 80] },
            didDrawPage: (data) => {
                doc.setFontSize(16);
                doc.text("Listado de Máquinas", 14, 30);
            }
        });

        const finalY = doc.autoTable.previous.finalY;
        doc.setFontSize(16);
        doc.text("Inventario de Repuestos", 14, finalY + 20);

        doc.autoTable({
            startY: finalY + 25,
            head: [['ID', 'Descripción', 'Stock', 'Costo Unitario']],
            body: state.parts.map(p => [p.id, p.description, p.stock, `$${p.cost.toFixed(2)}`]),
             headStyles: { fillColor: [44, 62, 80] },
        });

        doc.save("Reporte_General_Inventario.pdf");
        showToast('Reporte generado correctamente', 'success');
        showLoading(false);
    }, 1000);
}

function generateWorkOrderReport() {
    showLoading(true);
    setTimeout(() => {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        doc.text("Historial de Órdenes de Trabajo", 14, 20);
        doc.setFontSize(10);
        doc.text(`Fecha de generación: ${new Date().toLocaleDateString('es-ES')}`, 14, 26);

        const body = state.workOrders.map(o => {
            const machine = state.machines.find(m => m.id === o.machineId) || { name: 'N/A' };
            return [
                o.id,
                o.type,
                machine.name,
                o.status,
                new Date(o.date).toLocaleDateString('es-ES'),
                o.leadTechnician || 'N/A',
                o.supportTechnicians?.join(', ') || ''
            ];
        });

        doc.autoTable({
            startY: 35,
            head: [['ID', 'Tipo', 'Máquina', 'Estado', 'Fecha', 'Responsable', 'Apoyo']],
            body: body,
            headStyles: { fillColor: [44, 62, 80] },
            columnStyles: {
                2: { cellWidth: 40 }, 
                6: { cellWidth: 'auto'}
            }
        });

        doc.save("Reporte_Ordenes_de_Trabajo.pdf");
        showToast('Reporte generado correctamente', 'success');
        showLoading(false);
    }, 1000);
}

async function generateMachineReport() {
    const machineId = document.getElementById('report-machine-select').value;
    if (!machineId) {
        showToast('Seleccione una máquina para generar el reporte', 'warning');
        return;
    }
    
    showLoading(true);

    try {
        const machine = state.machines.find(m => m.id === machineId);
        if (!machine) {
            showToast('Máquina no encontrada.', 'error');
            showLoading(false);
            return;
        }
        const machineWorkOrders = state.workOrders.filter(o => o.machineId === machineId);

        // --- KPI Calculations ---
        const correctiveOrdersForKpi = machineWorkOrders
            .filter(o => o.type === 'Correctivo')
            .sort((a, b) => new Date(a.createdAt || a.date) - new Date(b.createdAt || b.date));
        
        let mtbf = 'N/A';
        if (correctiveOrdersForKpi.length > 1) {
            const firstFailureDate = new Date(correctiveOrdersForKpi[0].createdAt || correctiveOrdersForKpi[0].date);
            const lastFailureDate = new Date(correctiveOrdersForKpi[correctiveOrdersForKpi.length - 1].createdAt || correctiveOrdersForKpi[correctiveOrdersForKpi.length - 1].date);
            const totalTimeMs = lastFailureDate - firstFailureDate;
            if (totalTimeMs > 0) {
                const mtbfMs = totalTimeMs / (correctiveOrdersForKpi.length - 1);
                mtbf = `${parseFloat((mtbfMs / (1000 * 60 * 60 * 24)).toFixed(2))} días`;
            }
        }

        let mttr = 'N/A';
        const repairOrdersForKpi = correctiveOrdersForKpi.filter(o => o.status === 'Completado');
        if (repairOrdersForKpi.length > 0) {
            const totalRepairTimeMs = repairOrdersForKpi.reduce((sum, o) => sum + getTotalWorkDurationMs(o), 0);
            const mttrMs = totalRepairTimeMs / repairOrdersForKpi.length;
            mttr = `${parseFloat((mttrMs / (1000 * 60 * 60)).toFixed(2))} horas`;
        }

        // --- Chart Generation ---
        const chartContainer = document.createElement('div');
        chartContainer.style.position = 'absolute';
        chartContainer.style.left = '-9999px';
        chartContainer.style.width = '1000px';
        chartContainer.style.height = '400px';
        document.body.appendChild(chartContainer);

        const effectivenessCanvas = document.createElement('canvas');
        effectivenessCanvas.width = 300; effectivenessCanvas.height = 300;
        const failureTrendCanvas = document.createElement('canvas');
        failureTrendCanvas.width = 450; failureTrendCanvas.height = 300;
        chartContainer.appendChild(effectivenessCanvas);
        chartContainer.appendChild(failureTrendCanvas);

        const preventiveCount = machineWorkOrders.filter(o => o.type === 'Preventivo').length;
        const correctiveCount = machineWorkOrders.filter(o => o.type === 'Correctivo').length;

        const effectivenessChartPromise = new Promise((resolve) => {
             new Chart(effectivenessCanvas, {
                type: 'doughnut',
                data: {
                    labels: ['Preventivos', 'Correctivos'],
                    datasets: [{
                        data: [preventiveCount, correctiveCount],
                        backgroundColor: ['#3498db', '#e74c3c']
                    }]
                },
                options: {
                    responsive: false,
                    animation: { onComplete: () => resolve(effectivenessCanvas.toDataURL('image/png')) },
                    plugins: {
                        legend: { position: 'right' },
                        title: { display: true, text: 'Efectividad de Mantenimientos', font: { size: 14 } }
                    }
                }
            });
        });

        const failureTypes = ['Mecánica', 'Eléctrica', 'Electrónica', 'Falla de Operación'];
        const failureCounts = failureTypes.map(type => 
            correctiveOrdersForKpi.filter(o => o.failureType === type).length
        );
        
        const failureTrendChartPromise = new Promise((resolve) => {
            new Chart(failureTrendCanvas, {
                type: 'bar',
                data: {
                    labels: failureTypes,
                    datasets: [{
                        label: 'Cantidad de Fallas',
                        data: failureCounts,
                        backgroundColor: '#8e44ad',
                    }]
                },
                options: {
                    responsive: false,
                    animation: { onComplete: () => resolve(failureTrendCanvas.toDataURL('image/png')) },
                    plugins: {
                        legend: { display: false },
                        title: { display: true, text: 'Tendencia de Fallas Comunes', font: { size: 14 } }
                    },
                     scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }
                }
            });
        });
        
        const [effectivenessChartImg, failureTrendChartImg] = await Promise.all([effectivenessChartPromise, failureTrendChartPromise]);

        // --- PDF Generation ---
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        doc.setFontSize(18);
        doc.text(`Reporte Histórico de Máquina: ${machine.name}`, 14, 22);
        doc.setFontSize(12);
        doc.text(`ID: ${machine.id} | Ubicación: ${machine.location}`, 14, 30);
        
        // Add charts side-by-side
        doc.addImage(effectivenessChartImg, 'PNG', 14, 40, 70, 70);
        doc.addImage(failureTrendChartImg, 'PNG', 100, 40, 95, 60);

        const workOrderBody = machineWorkOrders.map(o => [
            o.id, o.type, o.type === 'Correctivo' ? o.failureType || 'N/A' : 'N/A',
            o.status, new Date(o.date).toLocaleDateString('es-ES'), o.description
        ]);
        
        doc.autoTable({
            startY: 115,
            head: [['ID Orden', 'Tipo', 'Tipo Falla', 'Estado', 'Fecha', 'Descripción']],
            body: workOrderBody,
            headStyles: { fillColor: [44, 62, 80] },
            columnStyles: { 5: { cellWidth: 'auto' } }
        });

        let finalY = doc.autoTable.previous.finalY;

        // --- KPIs, Availability, and Costs ---
        doc.addPage();
        doc.setFontSize(16);
        doc.text('Análisis Detallado', 14, 22);

        // KPI Table
        doc.setFontSize(12);
        doc.text('Indicadores Clave (Mantenimiento Correctivo)', 14, 32);
        doc.autoTable({
            startY: 35,
            body: [
                ['Tiempo Medio Entre Fallos (MTBF):', mtbf],
                ['Tiempo Medio de Reparación (MTTR):', mttr],
            ],
            theme: 'plain',
            styles: { fontSize: 10 }
        });
        finalY = doc.autoTable.previous.finalY;

        // Availability Table
        const availabilityBody = [];
        const processedDates = new Set();
        const completedCorrectives = machineWorkOrders.filter(o => o.type === 'Correctivo' && o.status === 'Completado' && o.date);
        
        completedCorrectives.forEach(o => {
            if (processedDates.has(o.date)) return;
            
            const dayDate = new Date(o.date + 'T12:00:00Z');
            const startDate = new Date(Date.UTC(dayDate.getUTCFullYear(), dayDate.getUTCMonth(), dayDate.getUTCDate(), 0, 0, 0));
            const endDate = new Date(Date.UTC(dayDate.getUTCFullYear(), dayDate.getUTCMonth(), dayDate.getUTCDate(), 23, 59, 59));
            const scheduledUptimeMs = calculateScheduledUptimeMs([machine], startDate, endDate);
            
            const downtimeOrdersThisDay = completedCorrectives.filter(wo => wo.date === o.date);
            const totalDowntimeMs = downtimeOrdersThisDay.reduce((sum, wo) => sum + getTotalWorkDurationMs(wo), 0);

            let availabilityText = 'N/A';
            if (scheduledUptimeMs > 0) {
                const availability = ((scheduledUptimeMs - totalDowntimeMs) / scheduledUptimeMs) * 100;
                availabilityText = `${Math.max(0, availability).toFixed(2)}%`;
            }
            availabilityBody.push([new Date(o.date + 'T12:00:00Z').toLocaleDateString('es-ES'), availabilityText]);
            processedDates.add(o.date);
        });

        if (availabilityBody.length > 0) {
            doc.text('Análisis de Disponibilidad por Día de Falla Correctiva', 14, finalY + 10);
            doc.autoTable({
                startY: finalY + 13,
                head: [['Fecha de Reparación', 'Disponibilidad del Día']],
                body: availabilityBody,
                headStyles: { fillColor: [44, 62, 80] },
            });
            finalY = doc.autoTable.previous.finalY;
        }

        // Cost Table
        const f = new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' });
        const completedOrders = machineWorkOrders.filter(o => o.status === 'Completado');
        const costBody = completedOrders.map(order => {
            const { partsCost, laborCost, totalCost } = calculateTotalCost(order, true);
            return [order.id, f.format(partsCost), f.format(laborCost), f.format(totalCost)];
        });

        if (costBody.length > 0) {
             doc.text('Análisis de Costos por Orden de Trabajo', 14, finalY + 10);
            doc.autoTable({
                startY: finalY + 13,
                head: [['ID Orden', 'Costo Repuestos', 'Costo Mano de Obra', 'Costo Total']],
                body: costBody,
                headStyles: { fillColor: [44, 62, 80] },
            });
            finalY = doc.autoTable.previous.finalY;
        }
        
        const { totalCost: grandTotalCost } = calculateTotalCostForMultiple(completedOrders);
        doc.setFontSize(12);
        doc.setFont(undefined, 'bold');
        doc.text(`Costo Total de Mantenimiento para esta Máquina: ${f.format(grandTotalCost)}`, 14, finalY + 15);

        doc.save(`Reporte_Maquina_${machine.id}.pdf`);
        
        // Cleanup
        document.body.removeChild(chartContainer);
        showToast(`Reporte de ${machine.name} generado`, 'success');

    } catch (error) {
        console.error("Error generating machine report:", error);
        showToast('Error al generar el reporte de máquina', 'error');
    } finally {
        showLoading(false);
    }
}

function generateMachinePartsReport() {
    const machineId = document.getElementById('report-machine-parts-select').value;
    if (!machineId) {
        showToast('Seleccione una máquina para generar el reporte', 'warning');
        return;
    }
    showLoading(true);

    setTimeout(() => {
        try {
            const machine = state.machines.find(m => m.id === machineId);
            if (!machine) {
                throw new Error("Máquina no encontrada.");
            }
            const partsForMachine = state.parts.filter(p => (p.machineIds && p.machineIds.includes(machineId)) || p.machineId === machineId);

            const { jsPDF } = window.jspdf;
            const doc = new jsPDF();

            doc.text(`Reporte de Repuestos para: ${machine.name} (${machine.id})`, 14, 20);
            doc.setFontSize(10);
            doc.text(`Fecha de generación: ${new Date().toLocaleDateString('es-ES')}`, 14, 26);

            const body = partsForMachine.map(p => [
                p.id,
                p.description,
                p.stock,
                p.minStock,
                p.location || 'N/A'
            ]);

            doc.autoTable({
                startY: 35,
                head: [['ID Repuesto', 'Descripción', 'Stock Actual', 'Stock Mínimo', 'Ubicación']],
                body: body,
                headStyles: { fillColor: [44, 62, 80] },
            });

            doc.save(`Reporte_Repuestos_${machine.id}.pdf`);
            showToast('Reporte de repuestos generado correctamente', 'success');
        } catch (error) {
            console.error("Error generating parts report:", error);
            showToast('Error al generar el reporte de repuestos.', 'error');
        } finally {
            showLoading(false);
        }
    }, 500);
}

function generateSingleWorkOrderReport() {
    const workOrderId = document.getElementById('report-wo-select').value;
    if (!workOrderId) {
        showToast('Seleccione una orden de trabajo para imprimir', 'warning');
        return;
    }
    
    showLoading(true);
    
    const order = state.workOrders.find(o => o.id === workOrderId);
    if (!order) {
        showToast('Orden de trabajo no encontrada.', 'error');
        showLoading(false);
        return;
    }

    const machine = state.machines.find(m => m.id === order.machineId) || { name: 'N/A', id: 'N/A' };

    setTimeout(() => { // Use timeout to allow loading spinner to show
        try {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('p', 'pt', 'letter');

            // --- Helper Functions ---
            const drawHeader = () => {
                doc.setFontSize(14);
                doc.setFont(undefined, 'bold');
                doc.text('Orden de Trabajo de Mantenimiento', doc.internal.pageSize.getWidth() / 2, 40, { align: 'center' });
                doc.setFontSize(10);
                doc.setFont(undefined, 'normal');
            };

            const drawBoxWithValue = (x, y, w, h, label, value) => {
                doc.rect(x, y, w, h);
                doc.setFontSize(8);
                doc.text(label, x + 3, y + 10);
                doc.setFontSize(10);
                doc.setFont(undefined, 'bold');
                doc.text(value || 'N/A', x + 3, y + 25);
                doc.setFont(undefined, 'normal');
            };

            // --- Document Body ---
            drawHeader();
            
            const pageWidth = doc.internal.pageSize.getWidth();
            const margin = 40;
            const contentWidth = pageWidth - (margin * 2);
            
            drawBoxWithValue(margin, 60, contentWidth * 0.4, 30, 'Código Máquina', machine.id);
            drawBoxWithValue(margin + contentWidth * 0.4 + 10, 60, contentWidth * 0.6 - 10, 30, 'Máquina', machine.name);
            
            doc.setFontSize(12);
            doc.setFont(undefined, 'bold');
            doc.text(`Número de Orden: ${order.id}`, margin, 115);

            drawBoxWithValue(margin, 130, contentWidth / 2 - 5, 30, 'Clase de Mantenimiento', order.type);
            drawBoxWithValue(margin + contentWidth / 2 + 5, 130, contentWidth / 2 - 5, 30, 'Tipo de Falla', order.failureType || 'N/A');

            const requestDate = order.createdAt ? new Date(order.createdAt).toLocaleDateString('es-ES') : 'N/A';
            drawBoxWithValue(margin, 170, contentWidth / 2 - 5, 30, 'Solicitado por:', order.requester || 'N/A');
            drawBoxWithValue(margin + contentWidth / 2 + 5, 170, contentWidth / 2 - 5, 30, 'Fecha de Solicitud:', requestDate);

            doc.rect(margin, 210, contentWidth, 80);
            doc.setFontSize(8);
            doc.text('Descripción de la actividad solicitada:', margin + 3, 220);
            doc.setFontSize(10);
            const descLines = doc.splitTextToSize(order.description, contentWidth - 10);
            doc.text(descLines, margin + 5, 235);
            
            const startDate = order.startTime ? new Date(order.startTime).toLocaleDateString('es-ES') : (order.date || 'N/A');
            const startTime = order.startTime ? new Date(order.startTime).toLocaleTimeString('es-ES', {hour: '2-digit', minute:'2-digit'}) : 'N/A';
            drawBoxWithValue(margin, 300, contentWidth / 2 - 5, 30, 'Asignado a:', order.leadTechnician || 'N/A');
            drawBoxWithValue(margin + contentWidth / 2 + 5, 300, contentWidth / 2 - 5, 30, 'Fecha de Inicio:', startDate);
            
            const collaborators = order.supportTechnicians ? order.supportTechnicians.join(', ') : 'N/A';
            drawBoxWithValue(margin, 340, contentWidth / 2 - 5, 30, 'Colaboradores:', collaborators);
            drawBoxWithValue(margin + contentWidth / 2 + 5, 340, contentWidth / 2 - 5, 30, 'Hora de Inicio:', startTime);

            const endDate = order.endTime ? new Date(order.endTime).toLocaleDateString('es-ES') : 'N/A';
            const totalHours = (getTotalWorkDurationMs(order) / (1000 * 60 * 60)).toFixed(2);
            drawBoxWithValue(margin, 380, contentWidth / 3 - 5, 45, 'Estado de la orden:', order.status);
            drawBoxWithValue(margin + contentWidth / 3, 380, contentWidth / 3 - 5, 45, 'Fecha Final:', endDate);
            drawBoxWithValue(margin + (contentWidth / 3) * 2, 380, contentWidth / 3, 45, 'Horas empleadas:', `${totalHours} hrs`);

            doc.setFontSize(12);
            doc.setFont(undefined, 'bold');
            doc.text('Repuestos / Materiales Utilizados', margin, 450);
            
            const partsBody = (order.partsUsed || []).map(pInfo => {
                const part = state.parts.find(p => p.id === pInfo.partId);
                return [
                    pInfo.partId,
                    part ? part.description : 'N/A',
                    pInfo.quantity,
                    part ? new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' }).format(part.cost) : 'N/A',
                    part ? new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' }).format(part.cost * pInfo.quantity) : 'N/A'
                ];
            });
            
            let finalY;
            if (partsBody.length > 0) {
                 doc.autoTable({
                    startY: 460,
                    head: [['ID', 'Descripción', 'Cantidad', 'Costo Unit.', 'Costo Total']],
                    body: partsBody,
                    headStyles: { fillColor: [44, 62, 80] },
                    margin: { left: margin, right: margin }
                });
                finalY = doc.autoTable.previous.finalY;
            } else {
                doc.setFontSize(10);
                doc.setFont(undefined, 'normal');
                doc.text('No se utilizaron repuestos para esta orden de trabajo.', margin, 470);
                finalY = 480;
            }

            doc.rect(margin, finalY + 20, contentWidth, 80);
            doc.setFontSize(8);
            doc.text('Observaciones / Trabajo Realizado:', margin + 3, finalY + 30);
            
            finalY = finalY + 110;

            doc.line(margin, finalY + 50, margin + (contentWidth / 2 - 20), finalY + 50);
            doc.text('Nombre y firma del técnico responsable', margin, finalY + 60);

            doc.line(margin + contentWidth / 2 + 20, finalY + 50, pageWidth - margin, finalY + 50);
            doc.text('Nombre y firma de quien recibe', margin + contentWidth / 2 + 20, finalY + 60);
            
            doc.save(`OT_${workOrderId}.pdf`);
            
        } catch (e) {
            console.error("Error generating single WO report:", e);
            showToast('Ocurrió un error al generar el PDF.', 'error');
        } finally {
            showLoading(false);
        }
    }, 500);
}

function handlePeriodChange() {
    const period = document.getElementById('dashboard-period-select').value;
    const dateSelect = document.getElementById('dashboardDate');
    const weekSelect = document.getElementById('dashboardWeek');
    const monthSelect = document.getElementById('dashboardMonth');
    const yearSelect = document.getElementById('dashboardYear');

    dateSelect.style.display = 'none';
    weekSelect.style.display = 'none';
    monthSelect.style.display = 'none';
    yearSelect.style.display = 'none';

    if (period === 'day') {
        dateSelect.style.display = 'inline-block';
    } else if (period === 'week') {
        populateWeekSelector();
        weekSelect.style.display = 'inline-block';
        monthSelect.style.display = 'inline-block';
        yearSelect.style.display = 'inline-block';
    } else if (period === 'month') {
        monthSelect.style.display = 'inline-block';
        yearSelect.style.display = 'inline-block';
    } else if (period === 'year') {
        yearSelect.style.display = 'inline-block';
    }
    updateDashboardData();
}

function getDashboardPeriod() {
    const period = document.getElementById('dashboard-period-select').value;
    let startDate, endDate, periodLabel;

    const dateSelect = document.getElementById('dashboardDate');
    const weekSelect = document.getElementById('dashboardWeek');
    const monthSelect = document.getElementById('dashboardMonth');
    const yearSelect = document.getElementById('dashboardYear');

    switch (period) {
        case 'day':
            const selectedDateVal = dateSelect.value;
            if (!selectedDateVal) {
                const now = new Date();
                startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
            } else {
                const selectedDate = new Date(selectedDateVal + 'T00:00:00');
                startDate = new Date(selectedDate.getFullYear(), selectedDate.getMonth(), selectedDate.getDate());
            }
            endDate = new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate(), 23, 59, 59);
            periodLabel = startDate.toLocaleDateString('es-ES', { day: 'numeric', month: 'long' });
            break;
        case 'week':
             if (weekSelect.value) {
                const [weekStartStr, weekEndStr] = weekSelect.value.split('|');
                startDate = new Date(weekStartStr + 'T00:00:00');
                endDate = new Date(weekEndStr + 'T23:59:59');
                periodLabel = `Semana del ${startDate.toLocaleDateString('es-ES')}`;
            } else { // Fallback
                const now = new Date();
                const firstDayOfWeek = now.getDate() - now.getDay();
                startDate = new Date(now.getFullYear(), now.getMonth(), firstDayOfWeek);
                endDate = new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate() + 6, 23, 59, 59);
                periodLabel = 'Esta Semana';
            }
            break;
        case 'year':
            const yearForYear = parseInt(yearSelect.value);
            startDate = new Date(yearForYear, 0, 1);
            endDate = new Date(yearForYear, 11, 31, 23, 59, 59);
            periodLabel = `Año ${yearForYear}`;
            break;
        case 'month':
        default:
            const month = parseInt(monthSelect.value);
            const yearForMonth = parseInt(yearSelect.value);
            startDate = new Date(yearForMonth, month, 1);
            endDate = new Date(yearForMonth, month + 1, 0, 23, 59, 59);
            const monthName = monthSelect.options[monthSelect.selectedIndex].text;
            periodLabel = `${monthName} ${yearForMonth}`;
            break;
    }
    return { startDate, endDate, periodLabel };
}

function updateDashboardData() {
    const { startDate, endDate, periodLabel } = getDashboardPeriod();

    document.querySelectorAll('.period-label').forEach(el => {
        el.textContent = `(${periodLabel})`;
    });

    let ordersForDashboard = state.workOrders;
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        ordersForDashboard = state.workOrders.filter(wo => managedIds.has(wo.machineId));
    }

    const ordersThisPeriod = ordersForDashboard.filter(o => {
        if (!o.date) return false;
        const orderDate = new Date(o.date + 'T12:00:00Z');
        return orderDate >= startDate && orderDate <= endDate;
    });

    updateStats(ordersThisPeriod);
    updateKpis(ordersThisPeriod, startDate, endDate);
    updateCharts(ordersThisPeriod);
}


// --- Dashboard & Charts ---
function calculateScheduledUptimeMs(machines, startDate, endDate) {
    let totalScheduledMs = 0;
    const oneDayMs = 24 * 60 * 60 * 1000;

    for (const machine of machines) {
        if (machine.scheduleDisabled) {
            continue; // Skip machines where schedule is disabled for availability calculation
        }

         const dayCount = Math.round((endDate.getTime() - startDate.getTime()) / oneDayMs) + 1 || 1;

        if (!machine.schedule) {
            totalScheduledMs += dayCount * 10 * 60 * 60 * 1000;
            continue;
        }

        let currentDate = new Date(startDate);
        while (currentDate.getTime() <= endDate.getTime()) {
            const dayOfWeek = currentDate.getDay();
            let dailySchedule = null;

            if (machine.schedule.weekday || machine.schedule.saturday || machine.schedule.sunday) {
                 if (dayOfWeek >= 1 && dayOfWeek <= 5) {
                    if (machine.schedule.weekday?.activeDays?.includes(String(dayOfWeek))) {
                        dailySchedule = machine.schedule.weekday;
                    }
                } else if (dayOfWeek === 6) {
                    if (machine.schedule.saturday?.active) {
                        dailySchedule = machine.schedule.saturday;
                    }
                } else if (dayOfWeek === 0) {
                    if (machine.schedule.sunday?.active) {
                        dailySchedule = machine.schedule.sunday;
                    }
                }
            } 

            if (dailySchedule && dailySchedule.startTime && dailySchedule.endTime) {
                const [startHour, startMinute] = dailySchedule.startTime.split(':').map(Number);
                const [endHour, endMinute] = dailySchedule.endTime.split(':').map(Number);
                const dailyUptimeMs = ((endHour * 60 + endMinute) - (startHour * 60 + startMinute)) * 60 * 1000;
                if (dailyUptimeMs > 0) {
                    totalScheduledMs += dailyUptimeMs;
                }
            }
            
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }
    return totalScheduledMs;
}

function calculateTotalCost(order, returnParts = false) {
    let partsCost = 0;
    if (order.partsUsed && order.partsUsed.length > 0) {
        partsCost = order.partsUsed.reduce((sum, partUsage) => {
            const part = state.parts.find(p => p.id === partUsage.partId);
            return sum + (part ? part.cost * partUsage.quantity : 0);
        }, 0);
    }

    const totalWorkMs = getTotalWorkDurationMs(order);
    const hoursWorked = totalWorkMs / (1000 * 60 * 60);
    
    let laborCost = 0;
    const techniciansOnOrder = order.technicians || [];
    if (techniciansOnOrder.length > 0 && hoursWorked > 0) {
        laborCost = techniciansOnOrder.reduce((sum, techUsername) => {
            const technician = state.technicians.find(t => t.username === techUsername);
            if (technician && technician.salario) {
                const hourlyRate = technician.salario / 160;
                return sum + (hoursWorked * hourlyRate);
            }
            return sum;
        }, 0);
    }

    const totalCost = partsCost + laborCost + (order.additionalCost || 0);
    return returnParts ? { partsCost, laborCost, totalCost } : totalCost;
}

function calculateTotalCostForMultiple(orders) {
    return orders.reduce((totals, order) => {
        const { partsCost, laborCost, totalCost } = calculateTotalCost(order, true);
        totals.partsCost += partsCost;
        totals.laborCost += laborCost;
        totals.totalCost += totalCost;
        return totals;
    }, { partsCost: 0, laborCost: 0, totalCost: 0 });
}


function updateStats(ordersThisPeriod) {
    document.getElementById('stat-maquinas').textContent = state.machines.length;
    document.getElementById('stat-solicitudes').textContent = state.solicitudes.filter(s => s.status === 'Pendiente').length;
    
    document.getElementById('stat-preventivos').textContent = ordersThisPeriod.filter(o => o.type === 'Preventivo').length;
    document.getElementById('stat-correctivos').textContent = ordersThisPeriod.filter(o => o.type === 'Correctivo').length;
    
    const f = new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' });

    // Gasto Ejecutado (Completed orders in period)
    const completedOrders = ordersThisPeriod.filter(o => o.status === 'Completado');
    const { totalCost: executedCost } = calculateTotalCostForMultiple(completedOrders);
    document.getElementById('stat-gasto-ejecutado').textContent = f.format(executedCost);
    
    // Gasto Planificado (All non-cancelled orders in period)
    const plannedOrders = ordersThisPeriod.filter(o => o.status !== 'Cancelado');
    const { totalCost: plannedCost } = calculateTotalCostForMultiple(plannedOrders);
    document.getElementById('stat-gasto-planificado').textContent = f.format(plannedCost);
}

function updateKpis(ordersInPeriod, startDate, endDate) {
    const machineId = document.getElementById('kpi-machine-select').value;
    
    const periodOrders = machineId === 'all' 
        ? ordersInPeriod 
        : ordersInPeriod.filter(wo => wo.machineId === machineId);

    // --- MTBF y MTTR para el período seleccionado ---
    const correctiveOrdersForPeriod = periodOrders.filter(o => o.type === 'Correctivo').sort((a,b) => new Date(a.createdAt || a.date) - new Date(b.createdAt || b.date));
    let mtbf = 'N/A';
    if(correctiveOrdersForPeriod.length > 1) {
        const firstFailureDate = new Date(correctiveOrdersForPeriod[0].createdAt || correctiveOrdersForPeriod[0].date);
        const lastFailureDate = new Date(correctiveOrdersForPeriod[correctiveOrdersForPeriod.length - 1].createdAt || correctiveOrdersForPeriod[correctiveOrdersForPeriod.length - 1].date);
        const timeBetweenFailures = lastFailureDate - firstFailureDate;

        if (timeBetweenFailures > 0) {
            const mtbfMs = timeBetweenFailures / (correctiveOrdersForPeriod.length - 1);
            mtbf = (mtbfMs / (1000 * 60 * 60 * 24)).toFixed(2);
        }
    }
    document.getElementById('kpi-mtbf').textContent = mtbf;
    
    const repairOrdersForPeriod = periodOrders.filter(o => o.type === 'Correctivo' && o.status === 'Completado');
    let mttr = 'N/A';
    if(repairOrdersForPeriod.length > 0) {
        const totalRepairTime = repairOrdersForPeriod.reduce((sum, o) => sum + getTotalWorkDurationMs(o), 0);
        const mttrMs = totalRepairTime / repairOrdersForPeriod.length;
        mttr = (mttrMs / (1000 * 60 * 60)).toFixed(2);
    }
    document.getElementById('kpi-mttr').textContent = mttr;
    
    // --- Disponibilidad para el período seleccionado ---
    let machinesForKpi = machineId === 'all' ? state.machines : [state.machines.find(m => m.id === machineId)].filter(Boolean);
    if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
        const managedIds = new Set(state.currentUser.managedMachineIds);
        machinesForKpi = machinesForKpi.filter(m => managedIds.has(m.id));
    }

    if (machinesForKpi.length > 0) {
        const scheduledUptimeMs = calculateScheduledUptimeMs(machinesForKpi, startDate, endDate);

        // El tiempo de inactividad proviene de órdenes correctivas completadas dentro del período
        const totalDowntimeMs = repairOrdersForPeriod.reduce((sum, o) => sum + getTotalWorkDurationMs(o), 0);
        
        let availabilityText = 'N/A';
        if (scheduledUptimeMs > 0) {
            const actualUptimeMs = Math.max(0, scheduledUptimeMs - totalDowntimeMs);
            const availabilityPercentage = (actualUptimeMs / scheduledUptimeMs) * 100;
            availabilityText = `${availabilityPercentage.toFixed(2)}%`;
        }
        document.getElementById('kpi-availability').textContent = availabilityText;
    } else {
        document.getElementById('kpi-availability').textContent = 'N/A';
    }

    // --- PMP y Costo (ya estaban correctos) ---
    const completedOrdersInPeriod = periodOrders.filter(o => o.status === 'Completado');
    let pmp = 'N/A';
    if (completedOrdersInPeriod.length > 0) {
        const preventiveCompleted = completedOrdersInPeriod.filter(o => o.type === 'Preventivo').length;
        pmp = ((preventiveCompleted / completedOrdersInPeriod.length) * 100).toFixed(2);
    }
    document.getElementById('kpi-pmp').textContent = pmp !== 'N/A' ? `${pmp}%` : 'N/A';

    let avgCost = 'N/A';
    if (completedOrdersInPeriod.length > 0) {
        const { totalCost } = calculateTotalCostForMultiple(completedOrdersInPeriod);
        avgCost = (totalCost / completedOrdersInPeriod.length);
    }
    document.getElementById('kpi-cost').textContent = avgCost !== 'N/A' ? new Intl.NumberFormat('es-HN', { style: 'currency', currency: 'HNL' }).format(avgCost) : 'N/A';
}

function updateCharts(ordersForPeriod) {
    if (!state.charts.maintenance) return;

    const preventiveCount = ordersForPeriod.filter(o => o.type === 'Preventivo').length;
    const correctiveCount = ordersForPeriod.filter(o => o.type === 'Correctivo').length;
    state.charts.maintenance.data.datasets[0].data = [preventiveCount, correctiveCount];
    state.charts.maintenance.update();

    const correctiveOrdersForPeriod = ordersForPeriod.filter(o => o.type === 'Correctivo');
    const failureCounts = {
        'Mecánica': correctiveOrdersForPeriod.filter(o => o.failureType === 'Mecánica').length,
        'Eléctrica': correctiveOrdersForPeriod.filter(o => o.failureType === 'Eléctrica').length,
        'Electrónica': correctiveOrdersForPeriod.filter(o => o.failureType === 'Electrónica').length,
        'Falla de Operación': correctiveOrdersForPeriod.filter(o => o.failureType === 'Falla de Operación').length,
    };
    state.charts.failureType.data.datasets[0].data = Object.values(failureCounts);
    state.charts.failureType.update();

    state.charts.taskStatus.data.datasets[0].data = [
        ordersForPeriod.filter(o => o.status === 'Pendiente').length,
        ordersForPeriod.filter(o => o.status === 'En Proceso').length,
        ordersForPeriod.filter(o => o.status === 'Completado').length,
        ordersForPeriod.filter(o => o.status === 'Cancelado').length
    ];
    state.charts.taskStatus.update();

    const trendLabels = [];
    const preventiveCounts = [];
    const correctiveCounts = [];
    
    const { startDate } = getDashboardPeriod();
    const baseDate = startDate;
    
    for (let i = 11; i >= 0; i--) {
        const d = new Date(baseDate.getFullYear(), baseDate.getMonth() - i, 1);
        trendLabels.push(d.toLocaleDateString('es-ES', { month: 'short', year: '2-digit' }));

        const preventiveInMonth = state.workOrders.filter(o => {
            if (!o.date) return false;
            const od = new Date(o.date + 'T12:00:00Z'); // Usar la fecha de inicio de la OT para el historial
            return o.type === 'Preventivo' && od.getUTCMonth() === d.getMonth() && od.getUTCFullYear() === d.getFullYear();
        }).length;
        preventiveCounts.push(preventiveInMonth);

        const correctiveInMonth = state.workOrders.filter(o => {
            if (!o.date) return false;
            const od = new Date(o.date + 'T12:00:00Z'); // Usar la fecha de inicio de la OT para el historial
            return o.type === 'Correctivo' && od.getUTCMonth() === d.getMonth() && od.getUTCFullYear() === d.getFullYear();
        }).length;
        correctiveCounts.push(correctiveInMonth);
    }
    state.charts.correctiveTrends.data.labels = trendLabels;
    state.charts.correctiveTrends.data.datasets[0].data = preventiveCounts;
    state.charts.correctiveTrends.data.datasets[1].data = correctiveCounts;
    state.charts.correctiveTrends.update();
}

function populateWorkOrderSelectors() {
    const selector = document.getElementById('report-wo-select');
    if (selector) {
        const currentVal = selector.value;
        selector.innerHTML = '<option value="">Seleccione una Orden de Trabajo...</option>';
        
        const sortedWOs = [...state.workOrders].sort((a, b) => b.id.localeCompare(a.id));

        sortedWOs.forEach(wo => {
            selector.innerHTML += `<option value="${wo.id}">${wo.id} - ${wo.description.substring(0, 30)}...</option>`;
        });
        selector.value = currentVal;
    }
}

function populateSupplierSelectors() {
    const selectors = [
        document.getElementById('part-supplier'),
    ];
    selectors.forEach(selector => {
        if (selector) {
            const currentVal = selector.value;
            selector.innerHTML = '<option value="">Seleccione un proveedor...</option>';
            state.proveedores.forEach(p => {
                selector.innerHTML += `<option value="${p.id}">${p.nombre}</option>`;
            });
            selector.value = currentVal;
        }
    });
}

function populateMachineSelectors() {
    const selectors = [
        document.getElementById('report-machine-select'),
        document.getElementById('report-machine-parts-select'),
        document.getElementById('kpi-machine-select'),
        document.getElementById('wo-machine'),
        document.getElementById('solicitud-machine')
    ];
    
    selectors.forEach(selector => {
        if (selector) {
            const isKpiSelector = selector.id === 'kpi-machine-select';
            const currentVal = selector.value;
            selector.innerHTML = isKpiSelector ? '<option value="all">Todas las Máquinas</option>' : '<option value="">Seleccione una máquina...</option>';
            
            let machinesToPopulate = state.machines;
            if (state.currentUser?.role === 'Jefe de Area' && Array.isArray(state.currentUser.managedMachineIds)) {
                const managedIds = new Set(state.currentUser.managedMachineIds);
                machinesToPopulate = state.machines.filter(m => managedIds.has(m.id));
            }

            machinesToPopulate.forEach(machine => {
                selector.innerHTML += `<option value="${machine.id}">${machine.id} - ${machine.name}</option>`;
            });
            selector.value = currentVal;
        }
    });
}
