document.addEventListener("DOMContentLoaded", function () {

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

  // Ask AI widget
  var script = document.createElement("script");
  script.type = "module";
  script.id = "runllm-widget-script"

  script.src = "https://widget.runllm.com";

  script.setAttribute("version", "stable");
  script.setAttribute("runllm-keyboard-shortcut", "Mod+j"); // cmd-j or ctrl-j to open the widget.
  script.setAttribute("runllm-name", "Daft");
  script.setAttribute("runllm-position", "BOTTOM_LEFT");
  script.setAttribute("runllm-position-x", "20px");
  script.setAttribute("runllm-position-y", "20px");
  script.setAttribute("runllm-assistant-id", "160");
  script.setAttribute("runllm-preset", "mkdocs");
  script.setAttribute("runllm-theme-color", "#7862ff");
  script.setAttribute("runllm-join-community-text", "Join Daft Slack!");
  script.setAttribute("runllm-community-url", "https://www.getdaft.io/slack.html?utm_source=docs&utm_medium=button&utm_campaign=docs_ask_ai");
  script.setAttribute("runllm-community-type", "slack");
  script.setAttribute("runllm-brand-logo", "https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/docs/img/favicon.png");

  script.async = true;
  document.head.appendChild(script);

  // Open external links in new tab
  var links = document.querySelectorAll('a[href^="http"], a[href^="https"]');
  var domain = window.location.hostname;

  for (var i = 0; i < links.length; i++) {
    // Check if the link doesn't point to your domain
    if (links[i].hostname !== domain) {
      links[i].target = "_blank";
      links[i].rel = "noopener noreferrer";
    }
  }

});
