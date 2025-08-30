// Daft Installation Tool JavaScript
document.addEventListener('DOMContentLoaded', function() {
  // Wait a bit to ensure the DOM is fully loaded
  setTimeout(function() {
    initializeInstallTool();
  }, 100);
});

function initializeInstallTool() {
  const checkboxes = document.querySelectorAll('#daft-install-tool input[type="checkbox"]');

  if (checkboxes.length === 0) {
    return;
  }

  checkboxes.forEach(checkbox => {
    checkbox.addEventListener('change', updateInstallCommand);
  });

  updateInstallCommand();
}

function updateInstallCommand() {
  const checkboxes = document.querySelectorAll('#daft-install-tool input[type="checkbox"]');
  const commandElement = document.getElementById('install-command');

  if (!commandElement) {
    return;
  }

  let extras = [];

  checkboxes.forEach(checkbox => {
    if (checkbox.checked) {
      const extra = checkbox.getAttribute('data-extra');
      extras.push(extra);
    }
  });

  let command = 'pip install -U ';
  command += 'daft';
  if (extras.length > 0) {
    // If all checkboxes are selected, use "all" instead of listing them all
    if (extras.length === checkboxes.length) {
      command += '[all]';
    } else {
      command += '[' + extras.join(',') + ']';
    }
    command = 'pip install -U "' + command.substring(15) + '"';
  }

  // Add syntax highlighting - only highlight quoted strings
  let highlightedCommand = command;
  if (command.includes('"daft[')) {
    // Highlight only the quoted package name
    highlightedCommand = command.replace(
      /(pip install -U )(")(daft\[[^\]]+\])(")/,
      '$1<span class="s2">$2$3$4</span>'
    );
  }

  commandElement.innerHTML = highlightedCommand;
}
