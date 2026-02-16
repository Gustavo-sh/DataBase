/* =============================
 * Filtro em cascata 
 * ============================= */
(function () {
    const gerenteSelect = document.getElementById("select-gerente");
    const operacaoSelect = document.getElementById("select-operacao");
    const atributoSelect = document.getElementById("atributo_select_cascata");

    if (!gerenteSelect || !operacaoSelect || !atributoSelect) {
    return;  // Sai sem quebrar outras páginas
}

    // Copiamos as options originais
    const operacoesOriginais = Array.from(operacaoSelect.options).slice(1);
    const atributosOriginais = Array.from(atributoSelect.options).slice(1);

    function filtrarOperacoes() {
        const gerente = gerenteSelect.value.toLowerCase();

        operacaoSelect.innerHTML = `<option value="">Todos</option>`;

        const filtradas = operacoesOriginais.filter(opt => {
            // pegar todas operações que pertencem ao gerente via ATTRIBUTOS
            // pois o select de operacao sozinho NÃO possui dataset
            const op = opt.value.toLowerCase();

            // EXISTE operação associada a esse gerente?
            return (
                gerente === "" ||
                atributosOriginais.some(attr =>
                    attr.dataset.gerente?.toLowerCase() === gerente &&
                    attr.dataset.operacao?.toLowerCase() === op
                )
            );
        });

        filtradas.forEach(opt => operacaoSelect.appendChild(opt));
    }

    function filtrarAtributos() {
        const gerente = gerenteSelect.value.toLowerCase();
        const operacao = operacaoSelect.value.toLowerCase();

        atributoSelect.innerHTML = `<option value="">Todos</option>`;

        const filtrados = atributosOriginais.filter(opt => {
            const g = opt.dataset.gerente?.toLowerCase() ?? "";
            const o = opt.dataset.operacao?.toLowerCase() ?? "";

            return (
                (gerente === "" || g === gerente) &&
                (operacao === "" || o === operacao)
            );
        });

        filtrados.forEach(opt => atributoSelect.appendChild(opt));
    }

    // Cascata
    gerenteSelect.addEventListener("change", () => {
        filtrarOperacoes();
        filtrarAtributos();
    });

    operacaoSelect.addEventListener("change", filtrarAtributos);

})();


/* =============================
 * 5) DMM duplicar: limite e bloqueio seguro
 * ============================= */
(function () {
  window.addEventListener("DOMContentLoaded", function () {
    const dmmDuplicar = document.querySelector("#dmm_apoio");
    if (!dmmDuplicar || typeof flatpickr === "undefined") return;

    function obterIntervaloDaTabela() {
      const linha = document.querySelector(".tabela-pesquisa tbody tr");
      if (!linha) return { inicio: null, fim: null };

      const celulas = linha.querySelectorAll("td");

      return {
        inicio: celulas[12]?.innerText.trim() || null,
        fim: celulas[13]?.innerText.trim() || null,
      };
    }

    flatpickr(dmmDuplicar, {
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

    function ultimoDiaDoMes(dataStr) {
      if (!dataStr) return null;
      const d = new Date(dataStr + "T12:00:00");
      return new Date(d.getFullYear(), d.getMonth() + 1, 0);
    }

    const checkDmmLimits = function () {
      const possui = document.querySelector("input[name='possuiDmm_apoio']:checked")?.value;

      const { inicio, fim } = obterIntervaloDaTabela();

      if (possui !== "Sim" || !inicio || !fim) {
        dmmDuplicar._flatpickr.set("clickOpens", false);
        return;
      }

      const ultimoDia = ultimoDiaDoMes(inicio);

      dmmDuplicar._flatpickr.set("minDate", inicio);
      dmmDuplicar._flatpickr.set("maxDate", ultimoDia);
      dmmDuplicar._flatpickr.set("clickOpens", true);
    };

    // 🔥 Quando o usuário clica no input → checar intervalo ANTES de abrir o calendário
    dmmDuplicar.addEventListener("mousedown", checkDmmLimits);

    // Quando escolher Sim/Não
    document.querySelectorAll("input[name='possuiDmm_apoio']").forEach((r) =>
      r.addEventListener("change", checkDmmLimits)
    );

    // Quando a tabela for atualizada pelo HTMX
    document.body.addEventListener("htmx:afterSwap", function (evt) {
      if (evt.target.id === "tabela-pesquisa") {
        checkDmmLimits();
      }
    });
  });
})();