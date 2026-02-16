/* matriz_apoio.js
 * Página: Apoio
 * Modo: Modular Parcial (usa matriz_core.js para blocos comuns)
 * Carregar DEPOIS de matriz_core.js
 


/* =============================
 * 4) Exportação customizada
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const exportBtn = document.getElementById("export-btn");
    if (!exportBtn) return;
    exportBtn.addEventListener("click", function () {
      const atributo = document.getElementById("atributo_select")?.value || "";
      const tipo = document.getElementById("duplicar_tipo_pesquisa")?.value || "";
      const cache_key = document.getElementById("cache_key_pesquisa")?.value || "";
      const params = new URLSearchParams();
      params.append("atributo", atributo);
      params.append("duplicar_tipo_pesquisa", tipo);
      params.append("cache_key", cache_key);
      const url = "/export_table?" + params.toString();
      window.open(url, "_blank");
    });
  });
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
