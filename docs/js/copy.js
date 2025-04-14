// This filters what gets copied to clipboard, given code line starts with >>>

// document.addEventListener("DOMContentLoaded", function () {
//     document.querySelectorAll(".highlight button").forEach((btn) => {
//       btn.addEventListener("click", function () {
//         const codeElement = btn.closest("div.highlight")?.querySelector("pre code");

//         if (!codeElement) return;

//         const rawText = codeElement.innerText;

//         const codeOnly = rawText
//           .split("\n")
//           .filter((line) => line.trim().startsWith(">>>") || line.trim().startsWith("..."))
//           .map((line) => line.replace(/^>>> ?/, "").replace(/^\.\.\. ?/, ""))
//           .join("\n");

//         navigator.clipboard.writeText(codeOnly).then(() => {
//           console.log("Copied only code input lines.");
//         });
//       });
//     });
//   });

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

        const codeLines = [];
        for (const line of lines) {
          if (isOutputMarker(line)) break;
          codeLines.push(line);
        }

        navigator.clipboard.writeText(codeLines.join("\n")).then(() => {
          console.log("Copied only code (stopped at first sign of output).");
        });
      });
    });
  });
