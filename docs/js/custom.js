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
