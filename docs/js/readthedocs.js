// Use CustomEvent to generate the version selector and show on click, not hover
document.addEventListener(
  "readthedocs-addons-data-ready",
  function (event) {
    // Check if version selector already exists
    if (document.querySelector('.md-version')) {
      return;
    }

    const config = event.detail.data();
    const versioning = `
            <div class="md-version">
              <button class="md-version__current" aria-label="Select version">
                ${config.versions.current.slug}
              </button>

              <ul class="md-version__list">
              ${config.versions.active.map(
      (version) => `
                <li class="md-version__item">
                  <a href="${version.urls.documentation}" class="md-version__link">
                    ${version.slug}
                  </a>
                </li>`).join("\n")}
              </ul>
            </div>`;

    document.querySelector(".md-header__topic").insertAdjacentHTML("beforeend", versioning);

    // Add click event handling for the version dropdown
    const versionButton = document.querySelector('.md-version__current');
    const versionList = document.querySelector('.md-version__list');

    versionButton.addEventListener('click', (e) => {
      e.stopPropagation();
      versionList.classList.toggle('md-version__list--active');
    });

    // Close dropdown when clicking outside
    document.addEventListener('click', () => {
      versionList.classList.remove('md-version__list--active');
    });
  });
