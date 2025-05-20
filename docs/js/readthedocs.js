// Use CustomEvent to generate the version selector
// document.addEventListener(
//         "readthedocs-addons-data-ready",
//         function (event) {
//           const config = event.detail.data();
//           const versioning = `
//             <div class="md-version">
//               <button class="md-version__current" aria-label="Select version">
//                 ${config.versions.current.slug}
//               </button>

//               <ul class="md-version__list">
//               ${ config.versions.active.map(
//                 (version) => `
//                 <li class="md-version__item">
//                   <a href="${ version.urls.documentation }" class="md-version__link">
//                     ${ version.slug }
//                   </a>
//                 </li>`).join("\n")}
//               </ul>
//             </div>`;

//           document.querySelector(".md-header__topic").insertAdjacentHTML("beforeend", versioning);
//  });

 // Use CustomEvent to generate the version selector
 document.addEventListener(
    "readthedocs-addons-data-ready",
    function (event) {
      const config = event.detail.data();

      // Create a sorted array of versions
      let sortedVersions = [...config.versions.active];

      // Remove 'latest' and 'stable' from the array if they exist
      const latestVersion = sortedVersions.find(v => v.slug === 'latest');
      const stableVersion = sortedVersions.find(v => v.slug === 'stable');

      sortedVersions = sortedVersions.filter(v => v.slug !== 'latest' && v.slug !== 'stable');

      // Sort remaining versions in reverse chronological order with proper semver handling
      sortedVersions.sort((a, b) => {
        // Extract version numbers (remove 'v' prefix if exists)
        const aVersion = a.slug.replace(/^v/, '');
        const bVersion = b.slug.replace(/^v/, '');

        // Parse version parts as individual numbers
        const aParts = aVersion.split('.').map(Number);
        const bParts = bVersion.split('.').map(Number);

        // Ensure parts arrays have equal length
        while (aParts.length < 3) aParts.push(0);
        while (bParts.length < 3) bParts.push(0);

        // Compare major version
        if (aParts[0] !== bParts[0]) return bParts[0] - aParts[0]; // Descending order
        // Compare minor version if majors are equal
        if (aParts[1] !== bParts[1]) return bParts[1] - aParts[1]; // Descending order
        // Compare patch version
        if (aParts[2] !== bParts[2]) return bParts[2] - aParts[2]; // Descending order

        // If there are more parts, continue comparing
        for (let i = 3; i < Math.max(aParts.length, bParts.length); i++) {
          const aVal = i < aParts.length ? aParts[i] : 0;
          const bVal = i < bParts.length ? bParts[i] : 0;
          if (aVal !== bVal) return bVal - aVal; // Descending order
        }

        return 0; // Versions are equal
      });

      // Create the final order with latest and stable first
      const orderedVersions = [];
      if (latestVersion) orderedVersions.push(latestVersion);
      if (stableVersion) orderedVersions.push(stableVersion);
      orderedVersions.push(...sortedVersions);

      const versioning = `
        <div class="md-version">
          <button class="md-version__current" aria-label="Select version">
            ${config.versions.current.slug}
          </button>

          <ul class="md-version__list">
          ${ orderedVersions.map(
            (version) => `
            <li class="md-version__item">
              <a href="${ version.urls.documentation }" class="md-version__link">
                ${ version.slug }
              </a>
            </li>`).join("\n")}
          </ul>
        </div>`;

      document.querySelector(".md-header__topic").insertAdjacentHTML("beforeend", versioning);
  });
