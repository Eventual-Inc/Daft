// Only copy code lines to clipboard, ignore output, True/False, and comments
document.addEventListener("DOMContentLoaded", function () {
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
});
