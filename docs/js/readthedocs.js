// Use CustomEvent to generate the version selector and inject CSS to hide readthedocs-flyout
document.addEventListener(
        "readthedocs-addons-data-ready",
        function (event) {
          const config = event.detail.data();
          const versioning = `
            <div class="md-version">
              <button class="md-version__current" aria-label="Select version">
                ${config.versions.current.slug}
              </button>

              <ul class="md-version__list">
              ${ config.versions.active.map(
                (version) => `
                <li class="md-version__item">
                  <a href="${ version.urls.documentation }" class="md-version__link">
                    ${ version.slug }
                  </a>
                </li>`).join("\n")}
              </ul>
            </div>`;

          document.querySelector(".md-header__topic").insertAdjacentHTML("beforeend", versioning);

          // Inject CSS to hide readthedocs-flyout
          const style = document.createElement('style');
          style.innerHTML = '.readthedocs-flyout { display: none; }';
          document.getElementsByTagName('head')[0].appendChild(style);
 });
