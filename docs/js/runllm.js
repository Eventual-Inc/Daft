document.addEventListener("DOMContentLoaded", function () {

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
  script.setAttribute("runllm-theme-color", "#ff00ff");
  script.setAttribute("runllm-join-community-text", "Join Daft Slack!");
  script.setAttribute("runllm-community-url", "https://www.getdaft.io/slack.html?utm_source=docs&utm_medium=button&utm_campaign=docs_ask_ai");
  script.setAttribute("runllm-community-type", "slack");
  script.setAttribute("runllm-brand-logo", "https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/docs/img/favicon.png");

  script.async = true;
  document.head.appendChild(script);

});
