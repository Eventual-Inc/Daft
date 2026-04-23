document.addEventListener('DOMContentLoaded', function () {
  const versionMatch = window.location.pathname.match(/^\/en\/([^/]+)\//);
  const currentVersion = versionMatch ? versionMatch[1] : 'stable';

  docsearch({
    container: '#docsearch',
    appId: 'CTX8NKR4GC',
    apiKey: 'eb05e80b7739efec7038d98158f39a54',
    indexName: 'getdaft',
    debug: false,
    insights: true,
    transformItems(items) {
      return items.map((item) => ({
        ...item,
        url: item.url.replace('/en/stable/', `/en/${currentVersion}/`),
      }));
    },
  });
});
