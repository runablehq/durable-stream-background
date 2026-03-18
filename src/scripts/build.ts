const result = await Bun.build({
  entrypoints: ["src/web/index.html"],
  outdir: "dist",
  minify: false,
  target: "browser",
});

if (!result.success) {
  console.error("Build failed:", result.logs);
  process.exit(1);
}

console.log("Built to dist/");
