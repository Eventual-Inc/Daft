# How to Install Daft

This guide helps you install Daft based on your specific needs and environment.

For most users, install Daft with a single command:

```bash
pip install -U daft
```

## Optional Dependencies

Depending on your use case, you may need to install Daft with additional dependencies.

<div id="daft-install-tool" class="daft-install-tool">
  <div class="use-cases">
    <h4>Use Cases:</h4>

    <div class="checkbox-group">
      <label class="checkbox-item">
        <input type="checkbox" id="huggingface" data-extra="huggingface">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Hugging Face</strong> <code>huggingface</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="openai" data-extra="openai">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>OpenAI</strong> <code>openai</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="sentence-transformers" data-extra="sentence-transformers">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Sentence Transformers</strong> <code>sentence-transformers</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="transformers" data-extra="transformers">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Transformers</strong> <code>transformers</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="ray" data-extra="ray">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Distributed Computing on Ray</strong> <code>ray</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="turbopuffer" data-extra="turbopuffer">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Turbopuffer</strong> <code>turbopuffer</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="aws" data-extra="aws">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>AWS Glue or AWS S3 Tables</strong> <code>aws</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="iceberg" data-extra="iceberg">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Apache Iceberg</strong> <code>iceberg</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="deltalake" data-extra="deltalake">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Delta Lake</strong> <code>deltalake</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="hudi" data-extra="hudi">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Apache Hudi</strong> <code>hudi</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="unity" data-extra="unity">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>Unity Catalog (Databricks)</strong> <code>unity</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="lance" data-extra="lance">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>LanceDB</strong> <code>lance</code>
        </div>
      </label>

      <label class="checkbox-item">
        <input type="checkbox" id="clickhouse" data-extra="clickhouse">
        <span class="checkmark"></span>
        <div class="checkbox-content">
          <strong>ClickHouse</strong> <code>clickhouse</code>
        </div>
      </label>
    </div>
  </div>

  <div class="command-output">
    <h4>Installation Command:</h4>
    <div class="highlight">
      <table class="highlighttable">
        <tbody>
          <tr>
            <td class="linenos">
              <div class="linenodiv">
                <pre><span></span><span class="normal">1</span></pre>
              </div>
            </td>
            <td class="code">
              <div>
                <pre><button class="md-clipboard md-icon" title="Copy to clipboard" data-clipboard-target="#install-command > code"></button><code id="install-command" class="language-bash">pip install -U daft</code></pre>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

You can also install Daft with all extra dependencies:

```bash
pip install -U "daft[all]"
```



## Troubleshooting Legacy CPU Support

If you encounter `Illegal instruction` errors, your CPU may lack support for advanced instruction sets like [AVX](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions). Use the LTS version instead:

```bash
pip install -U daft-lts
```

!!! warning "Performance Impact"
    The LTS version uses limited CPU instructions and cannot leverage vectorized operations, resulting in slower performance. Only use this if the standard package fails to run.

<style>
.daft-install-tool {
  margin: 20px 0;
}

.use-cases h4 {
  margin-top: 0;
  margin-bottom: 16px;
  color: var(--md-default-fg-color);
}

.checkbox-group {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 8px;
  margin-bottom: 24px;
  align-items: center;
}

.checkbox-item {
  display: flex;
  align-items: center;
  cursor: pointer;
  padding: 4px 4px 4px 16px;
  border: 1px solid var(--md-default-fg-color--lightest);
  border-radius: 6px;
  transition: all 0.2s ease;
}

.checkbox-item:hover {
  background: var(--md-default-fg-color--lightest);
  border-color: var(--md-accent-fg-color);
}

.checkbox-item input[type="checkbox"] {
  display: none;
}

.checkmark {
  width: 20px;
  height: 20px;
  border: 2px solid var(--md-default-fg-color--light);
  border-radius: 4px;
  margin-right: 12px;
  margin-top: 2px;
  position: relative;
  transition: all 0.2s ease;
}

.checkbox-item input[type="checkbox"]:checked + .checkmark {
  background: var(--md-accent-fg-color);
  border-color: var(--md-accent-fg-color);
}

.checkbox-item input[type="checkbox"]:checked + .checkmark::after {
  content: '✓';
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  color: white;
  font-size: 14px;
  font-weight: bold;
}

.checkbox-content {
  flex: 1;
}

.checkbox-content strong {
  display: block;
  margin-bottom: -4px;
  color: var(--md-default-fg-color);
  font-size: 14px;
  font-weight: normal;
}

.command-output h4 {
  margin-top: 0;
  margin-bottom: 12px;
  color: var(--md-default-fg-color);
}

.highlight {
  margin-bottom: 12px;
}
</style>
