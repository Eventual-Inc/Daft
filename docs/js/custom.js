// Function to process external links
function processExternalLinks() {
  var links = document.querySelectorAll('a[href^="http"], a[href^="https"]');
  var domain = window.location.hostname;

  for (var i = 0; i < links.length; i++) {
    // Check if the link doesn't point to your domain and is not inside a <header> or <footer>
    if (
      links[i].hostname !== domain &&
      !links[i].closest('header') && // Skip if inside a <header>
      !links[i].closest('footer')    // Skip if inside a <footer>
    ) {
      links[i].target = "_blank";
      links[i].rel = "noopener noreferrer";
      // Only append the arrow if it's not already there
      if (!links[i].innerHTML.includes('↗')) {
        links[i].innerHTML = links[i].innerHTML + ' <sup>↗</sup>';
      }
    }
  }
}

// Process links when DOM is loaded
document.addEventListener('DOMContentLoaded', processExternalLinks);

// Watch for dynamically added content
const observer = new MutationObserver(function(mutations) {
  mutations.forEach(function(mutation) {
    if (mutation.addedNodes.length) {
      processExternalLinks();
    }
  });
});

// Start observing the document with the configured parameters
observer.observe(document.body, {
  childList: true,
  subtree: true
});

// Only copy code lines to clipboard, ignore output, True/False, and comments
document.querySelectorAll(".highlight button").forEach((btn) => {
  btn.addEventListener("click", function () {
    const codeBlock = btn.closest("div.highlight")?.querySelector("pre code");
    if (!codeBlock) return;

    const lines = codeBlock.innerText.split("\n");

    const outputStartRegex = /^[╭╰│╞├┤┬┴─╌]/;
    const isOutputMarker = (line) =>
      outputStartRegex.test(line.trim()) ||
      (line.trim().startsWith("(") && line.includes("Showing"));

    const excludedPrefixes = ["True", "False", "#"];
    const shouldExcludeLine = (line) => {
      const trimmedLine = line.trim();
      return isOutputMarker(line) ||
             excludedPrefixes.some(prefix => trimmedLine.startsWith(prefix));
    };

    const codeLines = [];
    for (const line of lines) {
      if (isOutputMarker(line)) break;
      if (!shouldExcludeLine(line)) {
        codeLines.push(line);
      }
    }

    navigator.clipboard.writeText(codeLines.join("\n")).then(() => {
      console.log("Copied only code (excluded output, True/False, and comments).");
    });
  });
});

// Copy page as markdown functionality
(function() {
  function createCopyPageButton() {
    const sourceEl = document.getElementById("page-markdown-source");
    if (!sourceEl) return; // Only show button on pages with embedded markdown

    // Don't create duplicate buttons
    if (document.querySelector(".copy-page-btn")) return;

    const btn = document.createElement("button");
    btn.className = "copy-page-btn";
    btn.title = "Copy page as Markdown";
    btn.innerHTML = `
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16">
        <path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/>
      </svg>
      <span class="copy-page-btn-text">Copy page</span>
    `;

    btn.addEventListener("click", async function() {
      try {
        const markdown = JSON.parse(sourceEl.textContent);
        await navigator.clipboard.writeText(markdown);

        // Success feedback
        btn.querySelector(".copy-page-btn-text").textContent = "Copied";
        btn.classList.add("copied");

        setTimeout(() => {
          btn.querySelector(".copy-page-btn-text").textContent = "Copy page";
          btn.classList.remove("copied");
        }, 2000);
      } catch (error) {
        console.error("Failed to copy markdown:", error);
        btn.querySelector(".copy-page-btn-text").textContent = "Error";
        setTimeout(() => {
          btn.querySelector(".copy-page-btn-text").textContent = "Copy page";
        }, 2000);
      }
    });

    // Insert button next to the h1 title
    const h1 = document.querySelector(".md-content h1");
    if (h1) {
      // Create wrapper to position button inline with h1
      const wrapper = document.createElement("div");
      wrapper.className = "copy-page-header";
      h1.parentNode.insertBefore(wrapper, h1);
      wrapper.appendChild(h1);
      wrapper.appendChild(btn);
    }
  }

  // Initialize on page load and on navigation (for MkDocs instant loading)
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", createCopyPageButton);
  } else {
    createCopyPageButton();
  }

  // Re-run on instant navigation
  document.addEventListener("DOMContentSwitch", createCopyPageButton);
})();
