// Daft Installation Tool JavaScript.
document.addEventListener('DOMContentLoaded', function() {
  // Wait a bit to ensure the DOM is fully loaded.
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

  const prefix = 'pip install -U ';
  let packageName = 'daft';

  if (extras.length > 0) {
    // If all checkboxes are selected, use "all" instead of listing them all.
    if (extras.length === checkboxes.length) {
      packageName += '[all]';
    } else {
      packageName += '[' + extras.join(',') + ']';
    }
    // Quote the package name when it has extras.
    packageName = '"' + packageName + '"';
  }

  let command = prefix + packageName;

  commandElement.textContent = command;
}
