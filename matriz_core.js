/* matriz_core.js
 * Núcleo compartilhado entre páginas (Operação, ADM, etc.)
 * Script tradicional (sem type="module"), expõe utilitários no window.
 * Agrupado por tema. Tudo com guardas para só rodar se os elementos existirem.
 */

/* =============================
 * MENSAGENS CLEANER
 * ============================= */
window.__mensagemTimeout = null;

window.__mostrarToast = function (mensagem, tipo = "sucesso") {
    const container = document.getElementById("toast-global");
    if (!container) return;

    // criar toast
    const toast = document.createElement("div");
    toast.classList.add("toast");

    if (tipo === "erro") toast.classList.add("toast-erro");
    else if (tipo === "aviso") toast.classList.add("toast-aviso");
    else toast.classList.add("toast-sucesso");

    toast.innerHTML = mensagem;
    container.appendChild(toast);

    // pequena espera para animação
    setTimeout(() => toast.classList.add("show"), 20);

    // reset global timeout
    if (window.__mensagemTimeout) {
        clearTimeout(window.__mensagemTimeout);
    }

    window.__mensagemTimeout = setTimeout(() => {
        document.querySelectorAll("#toast-global .toast").forEach(t => {
            t.classList.remove("show");
            setTimeout(() => t.remove(), 350);
        });
    }, 8000);
};


/* =============================
 * HELPERS GERAIS
 * ============================= */
(function () {
  function debounce(fn, wait) {
    let t;
    return function (...args) {
      clearTimeout(t);
      t = setTimeout(() => fn.apply(this, args), wait);
    };
  }
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);
  window.__core__ = window.__core__ || {};
  window.__core__.debounce = debounce;
  window.__core__.$ = $;
  window.__core__.$$ = $$;
})();



/* =============================
 * FLATPICKR: datas + DMM (cadastro e duplicação)
 * ============================= */

function getInicioDuplicarRange() {
  const hoje = new Date();

  // Primeiro dia do mês atual
  const inicioPermitido = new Date(
    hoje.getFullYear(),
    hoje.getMonth(),
    1
  );

  // Último dia do mês seguinte
  const fimPermitido = new Date(
    hoje.getFullYear(),
    hoje.getMonth() + 2,
    0
  );

  return { inicioPermitido, fimPermitido };
}

(function () {
  let dataFimPicker;
  function initDataFim() {
    const el = document.getElementById("data_fim");
    if (!el) return;
    dataFimPicker = flatpickr(el, {
      dateFormat: "Y-m-d",
      locale: "pt",
      altInput: true,
      altFormat: "d/m/Y",
    });
  }

  const { inicioPermitido, fimPermitido } = getInicioDuplicarRange();

  function initDataInicio() {
    const el = document.getElementById("data_inicio");
    if (!el) return;
    flatpickr(el, {
      dateFormat: "Y-m-d",
      locale: "pt",
      minDate: inicioPermitido,
      maxDate: fimPermitido,
      altInput: true,
      altFormat: "d/m/Y",
      onChange: function (selectedDates) {
        if (selectedDates.length > 0 && dataFimPicker) {
          const inicio = selectedDates[0];
          const ultimoDia = new Date(inicio.getFullYear(), inicio.getMonth() + 1, 0);
          dataFimPicker.set("minDate", inicio);
          dataFimPicker.set("maxDate", ultimoDia);
        }
      },
    });
  }

  function createDmmPicker(selector) {
    const el = document.querySelector(selector);
    if (!el) return null;
    return flatpickr(el, {
      mode: "multiple",
      dateFormat: "Y-m-d",
      locale: "pt",
      altInput: true,
      altFormat: "d/m/Y",
      clickOpens: false,
      onChange: function (selectedDates, dateStr, instance) {
        if (selectedDates.length > 5) {
          selectedDates.pop();
          instance.setDate(selectedDates);
          alert("Você só pode selecionar no máximo 5 datas.");
        }
      },
    });
  }

  let dmmPicker, dmmPickerDuplicar;

  function updateDmmLimits() {
    const inicio = document.getElementById("data_inicio")?.value;
    const fim = document.getElementById("data_fim")?.value;
    const possuiDmm = document.querySelector("input[name='possuiDmm']:checked")?.value;
    if (!dmmPicker) return;
    if (inicio && fim && possuiDmm === "Sim") {
      dmmPicker.set("minDate", inicio);
      dmmPicker.set("maxDate", fim);
      dmmPicker.set("clickOpens", true);
    } else {
      dmmPicker.clear();
      dmmPicker.set("clickOpens", false);
    }
  }

  function updateDmmDuplicarLimits() {
    const inicio = document.getElementById("data_inicio_duplicar")?.value;
    const fim = document.getElementById("data_fim_duplicar")?.value;
    const possuiDmm = document.querySelector("input[name='possuiDmmDuplicar']:checked")?.value;
    if (!dmmPickerDuplicar) return;
    if (inicio && fim && possuiDmm === "Sim") {
      dmmPickerDuplicar.set("minDate", inicio);
      dmmPickerDuplicar.set("maxDate", fim);
      dmmPickerDuplicar.set("clickOpens", true);
    } else {
      dmmPickerDuplicar.clear();
      dmmPickerDuplicar.set("clickOpens", false);
    }
  }

  function initDuplicarDataPickers() {
    const inicioInput = document.getElementById("data_inicio_duplicar");
    const fimInput = document.getElementById("data_fim_duplicar");
    if (!inicioInput || !fimInput) return;

    const hoje = new Date();

    const anoAtual = hoje.getFullYear();
    const mesAtual = hoje.getMonth();

    const mesSeguinte = mesAtual === 11 ? 0 : mesAtual + 1;
    const anoSeguinte = mesAtual === 11 ? anoAtual + 1 : anoAtual;

    const diasInicioPermitidos = [1, 7, 8, 14, 15, 21, 22, 28, 30, 31];
    const diasFimPermitidos = [7, 14, 21, 28, 30, 31];

    // Limites absolutos
    const minInicio = new Date(anoAtual, mesAtual, 1);
    const maxInicio = new Date(
      anoSeguinte,
      mesSeguinte + 1,
      0
    );

    const fim = flatpickr(fimInput, {
      dateFormat: "Y-m-d",
      locale: "pt",
      clickOpens: false,
    });

    flatpickr(inicioInput, {
      dateFormat: "Y-m-d",
      locale: "pt",
      minDate: minInicio,
      maxDate: maxInicio,

      enable: [
        function (date) {
          const dia = date.getDate();
          const mes = date.getMonth();
          const ano = date.getFullYear();

          const ehMesAtual =
            mes === mesAtual && ano === anoAtual;

          const ehMesSeguinte =
            mes === mesSeguinte && ano === anoSeguinte;

          return (
            (ehMesAtual || ehMesSeguinte) &&
            diasInicioPermitidos.includes(dia)
          );
        },
      ],

      onChange: function (selectedDates, dateStr) {
        if (selectedDates.length === 0) return;

        const inicio = selectedDates[0];
        const ano = inicio.getFullYear();
        const mes = inicio.getMonth();

        const ultimoDiaMes = new Date(ano, mes + 1, 0);

        fim.set("minDate", inicio);
        fim.set("maxDate", ultimoDiaMes);

        fim.set("enable", [
          function (date) {
            return (
              date.getMonth() === mes &&
              date.getFullYear() === ano &&
              diasFimPermitidos.includes(date.getDate())
            );
          },
        ]);

        fim.set("clickOpens", true);

        if (window.syncDuplicarPeriodo)
          window.syncDuplicarPeriodo(dateStr);
      },
    });
  }

  function syncCamposDuplicar() {
    const inicioDuplicar = document.getElementById("data_inicio_duplicar");
    const fimDuplicar = document.getElementById("data_fim_duplicar");

    const dataInicioReadonly = document.getElementById("data_inicio");
    const dataFimReadonly = document.getElementById("data_fim");
    const periodoInput = document.getElementById("periodo_input");

    if (inicioDuplicar && dataInicioReadonly) {
      inicioDuplicar.addEventListener("change", function () {
        const valor = this.value;

        dataInicioReadonly.value = valor;
        dataFimReadonly.value = "";

        if (valor && periodoInput) {
          const dataObj = new Date(valor + " 12:00:00");

          if (!isNaN(dataObj)) {
            const ano = dataObj.getFullYear();
            const mes = String(dataObj.getMonth() + 1).padStart(2, "0");

            periodoInput.value = `${ano}-${mes}-01`;
          }
        }
      });
    }

    if (fimDuplicar && dataFimReadonly) {
      fimDuplicar.addEventListener("change", function () {
        dataFimReadonly.value = this.value;
      });
    }
  }


  function syncDuplicarPeriodo(dataInicioStr) {
    const periodoInput = document.getElementById("periodo_duplicar");
    if (periodoInput && dataInicioStr) {
      const dataObj = new Date(dataInicioStr + " 12:00:00");
      if (isNaN(dataObj)) {
        periodoInput.value = "";
        return;
      }
      const ano = dataObj.getFullYear();
      const mes = dataObj.getMonth() + 1;
      const mesFormatado = String(mes).padStart(2, "0");
      periodoInput.value = `${ano}-${mesFormatado}-01`;
    } else if (periodoInput) {
      periodoInput.value = "";
    }
  }

  window.addEventListener("DOMContentLoaded", function () {
    // initDataFim();
    // initDataInicio();
    // dmmPicker = createDmmPicker("#dmm");
    // dmmPickerDuplicar = createDmmPicker("#dmm_duplicar");

    // // listeners de limite DMM
    // ["#data_inicio", "#data_fim"].forEach((sel) => {
    //   const el = document.querySelector(sel);
    //   if (el) el.addEventListener("change", updateDmmLimits);
    // });
    // document.querySelectorAll("input[name='possuiDmm']").forEach((r) =>
    //   r.addEventListener("change", updateDmmLimits)
    // );

    // ["#data_inicio_duplicar", "#data_fim_duplicar"].forEach((sel) => {
    //   const el = document.querySelector(sel);
    //   if (el) el.addEventListener("change", updateDmmDuplicarLimits);
    // });
    // document.querySelectorAll("input[name='possuiDmmDuplicar']").forEach((r) =>
    //   r.addEventListener("change", updateDmmDuplicarLimits)
    // );

    initDuplicarDataPickers();
    syncCamposDuplicar();
  });

  window.syncDuplicarPeriodo = syncDuplicarPeriodo;
})();


/* =============================
 * CHOICES.JS: inicialização e sincronizações
 * ============================= */
(function () {
  let choicesAtributo;
  const selects = [
    "#indicadores",
    "#criterio_final",
    "#area",
    "#tipo_faturamento",
    "#atributo_select",
    "#escala_select",
    "#atributos_replicar",
    "#acordos_select",
    "#nao_acordos_exop_select",
    "#nao_acordos_apoio_select",
    "#adm_m0_select",
    "#adm_m1_select",
  ];

  function initChoices(selector) {
    const el = document.querySelector(selector);
    if (el && !el.dataset.choicesInitialized) {
      const instance = new Choices(el, {
      searchPlaceholderValue: "Selecione...",
      itemSelectText: "",
      shouldSort: false,
      position: "bottom",
      searchResultLimit: 15,
    });

    el.dataset.choicesInitialized = "true";

    if (selector === "#atributo_select") {
      choicesAtributo = instance;
      console.log("Instancia Choices para atributo_select criada no initChoices");
    }
    }
  }

  function bulkInit() {
    let lastInstance;
    selects.forEach((sel) => {
      const el = document.querySelector(sel);
      if (el) {
        lastInstance = new Choices(el, {
          searchPlaceholderValue: "Selecione...",
          itemSelectText: "",
          shouldSort: false,
          position: "bottom",
          searchResultLimit: 15,
        });
        el.dataset.choicesInitialized = "true";
      }
      if (sel === "#atributo_select") {
        choicesAtributo = lastInstance;
      }
    });
  }

  function syncAtributoHidden() {
    const atributoSelect = document.getElementById("atributo_select");
    const atributoHidden = document.getElementById("atributo_hidden");
    if (atributoSelect && atributoHidden) {
      atributoHidden.value = atributoSelect.value || "";
      atributoSelect.addEventListener("change", function () {
        atributoHidden.value = this.value || "";
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    bulkInit();
    initChoices("#operacao_select");
    initChoices("#atributo_select");
    syncAtributoHidden();
  });

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    const target = evt.detail?.target;
    if (!target) return;

    selects.forEach((sel) => {
      const el = target.querySelector(sel);
      if (el && !el.dataset.choicesInitialized) {
        const instance = new Choices(el, {
        searchPlaceholderValue: "Selecione...",
        itemSelectText: "",
        shouldSort: false,
        position: "bottom",
        searchResultLimit: 15,
      });

      el.dataset.choicesInitialized = "true";

      if (sel === "#atributo_select") {
        choicesAtributo = instance;

        window.__atributoBase__ = Array.from(el.options)
        .slice(1)
        .map(opt => ({
          value: opt.value,
          label: opt.textContent,
          daQualidade: opt.dataset.daQualidade,
          daPlanejamento: opt.dataset.daPlanejamento,
          daExop: opt.dataset.daExop,
          periodo: opt.dataset.periodo,
          tipo: opt.dataset.tipo,
          ativo: opt.dataset.ativo
        }));
      }

        if (sel === "#atributo_select") {
          syncAtributoHidden();
        }
      }
    });
  });

  window.__choicesAtributo__ = () => choicesAtributo;
  window.initChoices = initChoices;
  window.syncAtributoHidden = syncAtributoHidden;
})();


/* =============================
 * ATRIBUTO -> confirmação de limpeza (handleAtributoChange)
 * ============================= */
(function () {
  let lastSelectedAtributo = document.getElementById("atributo_select")?.value || "";

  function handleAtributoChange(selectElement) {
    const novoAtributo = selectElement.value;
    const registrosContainer = document.getElementById("registros");
    const temRegistros = registrosContainer && registrosContainer.children.length > 0;

    if (temRegistros) {
      const confirma = confirm(
        "Atenção! Ao mudar o atributo, você perderá todos os indicadores registrados na tabela. Deseja continuar?"
      );

      if (confirma) {
        htmx
          .ajax("POST", "/clear_registros", {
            target: registrosContainer,
            swap: "innerHTML",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
          })
          .then(() => {
            lastSelectedAtributo = novoAtributo;
            console.log("Registros zerados e atributo alterado para:", novoAtributo);
          })
          .catch((error) => {
            console.error("Erro ao limpar os registros:", error);
            alert("Erro ao limpar os registros no servidor. Tente novamente.");
            selectElement.value = lastSelectedAtributo;
          });
      } else {
        selectElement.value = lastSelectedAtributo;
        const ch = window.__choicesAtributo__ && window.__choicesAtributo__();
        if (ch && ch.setChoiceByValue) ch.setChoiceByValue(lastSelectedAtributo);
      }
    } else {
      lastSelectedAtributo = novoAtributo;
    }
    return false;
  }

  window.handleAtributoChange = handleAtributoChange;
})();

/* =============================
 * NORMALIZADOR UNIVERSAL
 * ============================= */
function normalizarMensagem(msg) {
    if (msg == null) return "";

    if (typeof msg === "string") return msg;

    // Caso seja { value: { detail: "..." } }
    if (msg.value && typeof msg.value === "object") {
        return msg.value.detail || JSON.stringify(msg.value);
    }

    // Caso seja { detail: "..." }
    if (msg.detail && typeof msg.detail === "string") {
        return msg.detail;
    }

    // Caso seja 422 com lista de erros
    if (Array.isArray(msg.detail)) {
        return msg.detail.map(e => e.msg || JSON.stringify(e)).join("<br>");
    }

    // Fallback
    return JSON.stringify(msg);
}

/* =============================
 * MOSTRAR SUCESSO / ERRO E 422
 * ============================= */
document.body.addEventListener("mostrarSucesso", function (evt) {
    let mensagem = normalizarMensagem(evt.detail?.value);
    if (!mensagem) return;

    window.__mostrarToast(mensagem, "sucesso");
});

document.body.addEventListener("mostrarErro", function (evt) {
    let mensagem = normalizarMensagem(evt.detail?.value);
    if (!mensagem) return;

    window.__mostrarToast(mensagem, "erro");
});

document.body.addEventListener("htmx:responseError", function (evt) {
    const xhr = evt.detail?.xhr;
    if (!xhr || xhr.status !== 422) return;

    let mensagem;

    try {
        mensagem = normalizarMensagem(JSON.parse(xhr.response));
    } catch {
        mensagem = xhr.response || "Erro 422 desconhecido.";
    }

    window.__mostrarToast(mensagem, "erro");
});

/* =============================
 * INDICADORES: change -> preenche campos e tipo do meta
 * ============================= */
(function () {
  function onIndicadorChange() {
    const sel = document.getElementById("indicadores");
    if (!sel) return;
    const opt = sel.options[sel.selectedIndex];
    if (!opt) return;

    let acumulado = opt.getAttribute("data-acumulado");
    let esquema = opt.getAttribute("data-esquema");
    let formato = opt.getAttribute("data-formato");

    if (esquema === "Diario") esquema = "Diário";

    const ac = document.getElementById("acumulado_input");
    const es = document.getElementById("esquema_input");
    const fm = document.getElementById("formato_input");
    if (ac) ac.value = acumulado || "";
    if (es) es.value = esquema || "";
    if (fm) fm.value = formato || "";

    const metaInput = document.getElementById("meta_input");
    if (metaInput) {
      metaInput.value = "";
      if (formato === "Decimal") {
        metaInput.type = "number";
        metaInput.step = "0.01";
        metaInput.placeholder = "Digite um número decimal";
      } else if (formato === "Percentual") {
        metaInput.type = "number";
        metaInput.step = "0.01";
        metaInput.placeholder = "Digite a % (ex: 75.5)";
      } else if (formato === "Inteiro") {
        metaInput.type = "number";
        metaInput.step = "1";
        metaInput.placeholder = "Digite um número inteiro";
      } else if (formato === "Hora") {
        metaInput.type = "time";
        metaInput.step = "1";
        metaInput.placeholder = "Digite a hora";
      } else {
        metaInput.type = "text";
        metaInput.placeholder = "Selecione um indicador";
      }
    }
  }

  window.addEventListener("DOMContentLoaded", function () {
    const sel = document.getElementById("indicadores");
    if (sel) sel.addEventListener("change", onIndicadorChange);
  });
})();


/* =============================
 * PERÍODO: a partir de data_inicio
 * ============================= */
(function () {
  window.addEventListener("DOMContentLoaded", function () {
    const dataInicio = document.getElementById("data_inicio");
    const periodo = document.getElementById("periodo_input");
    if (!dataInicio || !periodo) return;
    dataInicio.addEventListener("change", function () {
      if (dataInicio.value) {
        const [yyyy, mm] = dataInicio.value.split("-").map(Number);
        periodo.value = `${yyyy}-${String(mm).padStart(2, "0")}-01`;
      }
    });
  });
})();


/* =============================
 * ATRIBUTO -> gerente e tipo_matriz
 * ============================= */
(function () {
  function bindAtributoToGerente() {
    const atributoSelect = document.getElementById("atributo_select");
    const gerenteInput = document.getElementById("gerente_input");
    const tipoInput = document.getElementById("tipomatriz_select");

    if (atributoSelect && gerenteInput) {
      atributoSelect.addEventListener("change", function () {
        const opt = this.options[this.selectedIndex];
        gerenteInput.value = opt ? opt.dataset.gerente || "" : "";
      });
    }
    if (atributoSelect && tipoInput) {
      atributoSelect.addEventListener("change", function () {
        const opt = this.options[this.selectedIndex];
        tipoInput.value = opt ? opt.dataset.tipo || "" : "";
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (window.initChoices) window.initChoices("#atributo_select");
    if (window.syncAtributoHidden) window.syncAtributoHidden();
    bindAtributoToGerente();
    if (window.initDuplicarDataPickers) window.initDuplicarDataPickers();
  });

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    if (evt.detail.target.id === "tabela-pesquisa") {

      const url = evt.detail.pathInfo.requestPath;

      console.log("URL após swap:", url);

      let modo = "";

      if (url.includes("all_atributes_operacao")) {
        modo = "all_atributes_operacao";
      } else if (url.includes("pesquisar_mes")) {
        modo = "pesquisar_mes";
      }

      let inputModo = document.getElementById("modo_pesquisa_atual");

      if (!inputModo) {
        return;
      }

      if (modo !== "" && modo !== undefined) {
        inputModo.value = modo;
        console.log("Modo definido: " + modo);
      }
    }
    if (evt.detail?.target?.id === "atributos-container") {
      if (window.initChoices) window.initChoices("#atributo_select");
      if (window.syncAtributoHidden) window.syncAtributoHidden();
      bindAtributoToGerente();
    }
  });
})();


/* =============================
 * BLOCO: SELECT-ALL na TABELA DE PESQUISA
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const isApoioCadastro = window.location.pathname.toLowerCase().includes("cadastro");
  if (isApoioCadastro) return; // Não habilita select-all aqui
    const selectAllButton = document.getElementById("selecionar-tudo-btn");
    if (!selectAllButton) return;

    selectAllButton.addEventListener("click", function () {
      const checkboxes = document.querySelectorAll('input[name="registro_ids"]');
      const shouldSelect = Array.from(checkboxes).some((cb) => !cb.checked);
      checkboxes.forEach((cb) => (cb.checked = shouldSelect));
      selectAllButton.textContent = shouldSelect ? "Desmarcar Tudo" : "Selecionar Tudo";
    });
  });
})();


/* =============================
 * EXPORTAÇÃO padrão
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const btn = document.getElementById("export-btn");
    const urlAtual = window.location.pathname;
    let page = null;
    if (urlAtual.includes("cadastro")) {
      page = "cadastro";
    } else {
      page = "demais";
    }
    if (!btn) return;
    btn.addEventListener("click", function () {
      const atributo = document.getElementById("atributo_select")?.value || "";
      const tipo = document.getElementById("duplicar_tipo_pesquisa")?.value || "";
      const cache_key = document.getElementById("cache_key_pesquisa")?.value || "";
      const modo = document.getElementById("modo_pesquisa_atual")?.value;
      const params = new URLSearchParams();
      params.append("tipo_pesquisa", tipo);
      params.append("atributo", atributo);
      params.append("page", page);
      console.log("Modo no params da exportação: " + modo);
      params.append("modo", modo);
      const url = "/export_table?" + params.toString();
      window.open(url, "_blank");
    });
  });
})();


/* =============================
 * HTMX config: persistir tipo_pesquisa ao clicar em ALL
 * ============================= */
(function () {
  document.addEventListener("htmx:configRequest", function (event) {
    const element = event.detail?.elt;
    const id = element?.id;
    if (id === "all_m0" || id === "all_m1" || id === "all_m+1") {
      const tipoPesquisa = event.detail.parameters?.["tipo_pesquisa"];
      const hidden = document.getElementById("duplicar_tipo_pesquisa");
      if (hidden) hidden.value = tipoPesquisa;
    }
  });
})();


/* =============================
 * LOADER ROBUSTO PARA HTMX
 * Suporta: swap, no-swap, outerHTML, abort, error, dblclick, etc.
 * ============================= */
(function () {
  const overlay = document.getElementById("overlay");
  const loader  = document.getElementById("loader");
  if (!overlay || !loader) return;

  let inflight = 0;
  let failSafeTimer = null;

  function showLoader() {
    inflight++;
    overlay.style.display = "block";
    loader.style.display  = "block";

    // failsafe: se algo travar, desmonta após 12s
    clearTimeout(failSafeTimer);
    failSafeTimer = setTimeout(() => {
      inflight = 0;
      hideLoader();
    }, 12000);
  }

  function hideLoader() {
    if (inflight > 0) inflight--;
    if (inflight === 0) {
      overlay.style.display = "none";
      loader.style.display  = "none";
      clearTimeout(failSafeTimer);
    }
  }

  // Início de qualquer requisição HTMX
  document.body.addEventListener("htmx:beforeRequest", showLoader);

  // Fim quando conteúdo chegou e foi aplicado
  document.body.addEventListener("htmx:afterSwap", hideLoader);

  // Quando resposta chega mas não gera swap
  document.body.addEventListener("htmx:afterRequest", hideLoader);

  // Em erro de response
  document.body.addEventListener("htmx:responseError", hideLoader);

  // Em erro de request antes de chegar ao servidor
  document.body.addEventListener("htmx:requestError", hideLoader);

  // Quando requisição é abortada
  document.body.addEventListener("htmx:abort", hideLoader);
})();

/* =============================
 * SINCRONIZA DUPLICAÇÃO DE PESQUISA
 * - Mes (m0, m1, m+1) -> #duplicar_tipo_pesquisa
 * - Atributo -> #duplicar_atributo
 * ============================= */
(function () {

  function sincronizarDuplicacao(event) {
    const botao = event.target.closest("button");
    if (!botao) return;

    const campoTipo = document.getElementById("duplicar_tipo_pesquisa");
    const campoAtributoDuplicar = document.getElementById("duplicar_atributo");
    const selectAtributo = document.getElementById("atributo_select");

    if (!campoTipo || !campoAtributoDuplicar || !selectAtributo) return;

    // --- captura mês pelo hx-vals ---
    const hxVals = botao.getAttribute("hx-vals");

    if (hxVals) {
      try {
        const valores = JSON.parse(hxVals);

        if (valores.mes) {
          campoTipo.value = valores.mes;
          console.log("Tipo de pesquisa setado: ", valores.mes);
        }

      } catch (error) {
        console.error("Erro ao interpretar hx-vals:", error);
      }
    }

    // --- sincroniza atributo atual ---
    campoAtributoDuplicar.value = selectAtributo.value;
    console.log("Atributo sincronizado: ", selectAtributo.value);
  }

  // Delegação global segura
  document.body.addEventListener("click", function (event) {
    if (
      event.target.closest("#btn-m0") ||
      event.target.closest("#btn-m1") ||
      event.target.closest("#btn-mmais1")
    ) {
      sincronizarDuplicacao(event);
    }
  });

})();

(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    if (evt?.detail?.xhr?.status === 401) {
      window.location.href = "/login";
    }
  });
})();

(function () {
  const btn = document.getElementById("app-launcher-btn");
  const panel = document.getElementById("app-launcher-panel");
  const backdrop = document.getElementById("launcher-backdrop");
  const closeBtn = document.getElementById("close-launcher");

  if (!btn || !panel || !backdrop || !closeBtn) return;

  // Abre
  btn.addEventListener("click", () => {
    panel.classList.add("open");
    backdrop.classList.add("active");
  });

  // Fecha no X
  closeBtn.addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("active");
  });

  // Fecha clicando fora
  backdrop.addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("active");
  });
})();

(function () {
  const btn = document.getElementById("kpi-launcher-btn");
  const panel = document.getElementById("kpi-launcher-panel");
  const backdrop = document.getElementById("launcher-backdrop");
  const closeBtn = document.getElementById("close-launcher");

  if (!btn || !panel || !backdrop || !closeBtn) return;

  // Abre
  btn.addEventListener("click", () => {
    panel.classList.add("open");
    backdrop.classList.add("active");
  });

  // Fecha no X
  closeBtn.addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("active");
  });

  // Fecha clicando fora
  backdrop.addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("active");
  });
})();

function aplicarFiltroAtributo(regraFiltro) {

  const instance = window.__choicesAtributo__?.();
  const base = window.__atributoBase__;

  if (!instance || !base) return;

  const filtrados = base.filter(regraFiltro);

  instance.clearChoices();

  instance.setChoices(
    filtrados.map(item => ({
      value: item.value,
      label: item.label,
      selected: false,
      disabled: false
    })),
    "value",
    "label",
    false
  );
}

(function () {
  const url = window.location.pathname.toLowerCase();
  if (!url.includes("/matriz/adm/acordo")) {
    return;
  }

  document.getElementById("btn_atributos_da_apoio")
    .addEventListener("click", function () {

      aplicarFiltroAtributo(item =>
        item.daQualidade === "1" &&
        item.daPlanejamento === "1" &&
        item.ativo === "0"
      );

  });

  document.getElementById("btn_atributos_na_apoio")
    .addEventListener("click", function () {

      aplicarFiltroAtributo(item =>
        (item.daQualidade === "2" ||
        item.daPlanejamento === "2") &&
        item.ativo === "0"
      );

  });

  document.getElementById("btn_atributos_na_exop")
    .addEventListener("click", function () {

      aplicarFiltroAtributo(item =>
        item.daExop === "2" && item.ativo === "0"
      );

  });

  document.getElementById("btn_atributos_adm")
    .addEventListener("click", function () {

      aplicarFiltroAtributo(item => {
        if (!item.tipo) return false;

        const tipoNormalizado = item.tipo
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "") // remove acentos
          .trim()
          .toUpperCase();
        

        return tipoNormalizado === "ADMINISTRACAO" && item.periodo !== "" && item.ativo === "0";
      });

  });

  document.getElementById("btn_reset_atributos")
    .addEventListener("click", function () {


      aplicarFiltroAtributo(() => true);


  });
})();
