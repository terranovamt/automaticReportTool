window.addEventListener('load', function () {
  var loading = document.querySelector('.loading');
  if (loading) {
    loading.style.display = 'none';
  }
});

let calcScrollValue = () => {
  let scrollProgress = document.getElementById('progress');
  let progressValue = document.getElementById('progress-value');

  if (!scrollProgress) return;

  let tableOfContent = document.getElementById('TableOfContent');
  if (!tableOfContent) return;

  // Posizione corrente dello scroll
  let pos = document.documentElement.scrollTop;

  // Posizione dell'inizio del TableOfContent
  let tocStart = tableOfContent.offsetTop;

  // Altezza totale del documento
  let docHeight = document.documentElement.scrollHeight;

  // Altezza della viewport
  let viewportHeight = window.innerHeight;

  // Altezza effettiva scrollabile dalla posizione del TableOfContent alla fine
  let scrollableHeight = docHeight - tocStart - viewportHeight;

  // Distanza scrollata dal punto di inizio del TableOfContent
  let scrolledFromToc = Math.max(0, pos - tocStart);

  // Calcolo della percentuale (limitata tra 0 e 100)
  let scrollValue = scrollableHeight > 0 ? Math.min(100, Math.max(0, Math.round((scrolledFromToc / scrollableHeight) * 100))) : 0;

  // Mostra/nascondi l'indicatore
  if (pos > tocStart) {
    scrollProgress.style.display = 'grid';
  } else {
    scrollProgress.style.display = 'none';
  }

  // Aggiorna lo stile del progresso
  scrollProgress.style.background = `conic-gradient(#03234B ${scrollValue}%, #d7d7d7 ${scrollValue}%)`;

  // Debug (rimuovi in produzione)
  // console.log(`Pos: ${pos}, TocStart: ${tocStart}, ScrolledFromToc: ${scrolledFromToc}, ScrollableHeight: ${scrollableHeight}, Progress: ${scrollValue}%`);
};

// Gestione del click per tornare al TableOfContent
let addScrollProgressClickHandler = () => {
  let scrollProgress = document.getElementById('progress');
  if (scrollProgress) {
    // Rimuovi eventuali listener precedenti
    scrollProgress.removeEventListener('click', scrollToTableOfContent);
    scrollProgress.addEventListener('click', scrollToTableOfContent);
  }
};

let scrollToTableOfContent = () => {
  let tableOfContent = document.getElementById('TableOfContent');
  if (tableOfContent) {
    tableOfContent.scrollIntoView({ behavior: 'smooth' });
  }
};

// Inizializzazione
let tableOfContent = document.getElementById('TableOfContent');

if (tableOfContent) {
  let observer = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting) {
        // Usa onscroll come nel codice originale per l'aggiornamento continuo
        window.onscroll = calcScrollValue;
        window.onload = calcScrollValue;
        addScrollProgressClickHandler();
        // Calcola subito il valore iniziale
        calcScrollValue();
      }
    },
    {
      // Opzioni per l'observer
      threshold: 0.1,
      rootMargin: '0px',
    }
  );

  observer.observe(tableOfContent);
} else {
  console.warn('Elemento TableOfContent non trovato');
}
