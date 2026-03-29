import { useEffect, useRef } from "react"
import { EditorView, keymap } from "@codemirror/view"
import { Compartment, EditorState } from "@codemirror/state"
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands"
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language"
import { tags } from "@lezer/highlight"
import { cn } from "@/lib/utils"
import { oswsLanguage } from "@/lib/oswsLanguage"

// Uses shadcn CSS variables so it adapts to the active theme automatically.
const highlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: "var(--primary)" },                   // GRANT / REVOKE / CREATE / DROP
  { tag: tags.typeName, color: "oklch(0.72 0.14 210)" },            // ROLE / ACCESS / ON / TO / FROM
  { tag: tags.comment, color: "var(--muted-foreground)", fontStyle: "italic" },
  { tag: tags.variableName, color: "var(--foreground)" },
  { tag: tags.punctuation, color: "var(--muted-foreground)" },
])

const editorTheme = EditorView.theme({
  "&": {
    fontSize: "0.875rem",
    fontFamily: "var(--font-mono, monospace)",
  },
  ".cm-content": {
    padding: "12px",
    caretColor: "var(--foreground)",
  },
  ".cm-focused": {
    outline: "none",
  },
  ".cm-cursor, .cm-dropCursor": {
    borderLeftColor: "var(--foreground)",
  },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
    backgroundColor: "oklch(0.69 0.19 293 / 30%)",
  },
  "::selection": {
    backgroundColor: "oklch(0.69 0.19 293 / 30%)",
  },
  ".cm-placeholder": {
    color: "var(--muted-foreground)",
  },
  ".cm-scroller": {
    fontFamily: "inherit",
    minHeight: "180px",
  },
})

interface QueryEditorProps {
  value: string
  onChange: (v: string) => void
  onRun: () => void
  disabled?: boolean
  placeholder?: string
  className?: string
}

export function QueryEditor({
  value,
  onChange,
  onRun,
  disabled,
  placeholder,
  className,
}: QueryEditorProps) {
  const wrapperRef = useRef<HTMLDivElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)
  const onChangeRef = useRef(onChange)
  const onRunRef = useRef(onRun)
  const editableRef = useRef(new Compartment())

  onChangeRef.current = onChange
  onRunRef.current = onRun

  useEffect(() => {
    if (!containerRef.current) return

    const view = new EditorView({
      parent: containerRef.current,
      state: EditorState.create({
        doc: value,
        extensions: [
          history(),
          keymap.of([
            { key: "Mod-Enter", run: () => { onRunRef.current(); return true } },
            ...historyKeymap,
            ...defaultKeymap,
          ]),
          oswsLanguage,
          syntaxHighlighting(highlightStyle),
          EditorView.lineWrapping,
          editableRef.current.of(EditorView.editable.of(!disabled)),
          EditorView.updateListener.of(update => {
            if (update.docChanged) onChangeRef.current(update.state.doc.toString())
          }),
          editorTheme,
        ],
      }),
    })

    viewRef.current = view
    return () => {
      view.destroy()
      viewRef.current = null
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Sync external value changes (e.g., after run clears the input)
  useEffect(() => {
    const view = viewRef.current
    if (!view || view.state.doc.toString() === value) return
    view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } })
  }, [value])

  // Sync disabled prop
  useEffect(() => {
    viewRef.current?.dispatch({
      effects: editableRef.current.reconfigure(EditorView.editable.of(!disabled)),
    })
  }, [disabled])

  return (
    <div
      ref={wrapperRef}
      className={cn(
        "relative w-full bg-muted/40 border rounded focus-within:outline-none focus-within:ring-2 focus-within:ring-ring",
        disabled && "opacity-50 pointer-events-none",
        className,
      )}
    >
      {!value && placeholder && (
        <div
          aria-hidden
          className="absolute inset-0 p-3 font-mono text-sm text-muted-foreground whitespace-pre pointer-events-none select-none"
        >
          {placeholder}
        </div>
      )}
      <div ref={containerRef} />
    </div>
  )
}
